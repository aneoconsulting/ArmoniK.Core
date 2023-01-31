// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Runtime.CompilerServices;
using System.Text;

using Amazon.S3;
using Amazon.S3.Model;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using CommunityToolkit.HighPerformance;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.S3;

public class ObjectStorage : IObjectStorage
{
  private const    int                    MaxAllowedKeysPerDelete = 1000;
  private readonly string                 bucketName_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly AmazonS3Client         s3Client_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for S3
  /// </summary>
  /// <param name="s3Client">Connection to S3</param>
  /// <param name="objectStorageName">Name of the object storage used to differentiate them</param>
  /// <param name="options">S3 object storage options</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(AmazonS3Client         s3Client,
                       string                 objectStorageName,
                       Options.S3             options,
                       ILogger<ObjectStorage> logger)
  {
    s3Client_          = s3Client;
    objectStorageName_ = objectStorageName;
    bucketName_        = options.BucketName;
    logger_            = logger;
  }

  public async Task AddOrUpdateAsync(string                   key,
                                     IAsyncEnumerable<byte[]> valueChunks,
                                     CancellationToken        cancellationToken = default)
    => await AddOrUpdateAsync(key,
                              valueChunks.Select(elem => new ReadOnlyMemory<byte>(elem))
                                         .AsAsyncEnumerable(),
                              cancellationToken);

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string                                 key,
                                     IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                     CancellationToken                      cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);

    var idx      = 0;
    var taskList = new List<Task>();
    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      taskList.Add(s3Client_.WriteObjectAsync(bucketName_,
                                              $"{objectStorageName_}{key}_{idx}",
                                              chunk));
      ++idx;
    }

    await s3Client_.WriteStringAsync(bucketName_,
                                     $"{objectStorageName_}{key}_count",
                                     idx.ToString())
                   .ConfigureAwait(false);
    await taskList.WhenAll()
                  .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(string                                     key,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var          _        = logger_.LogFunction(objectStorageName_ + key);
    GetObjectResponse? response = null;
    try
    {
      response = await s3Client_.GetObjectAsync(bucketName_,
                                                $"{objectStorageName_}{key}_count",
                                                cancellationToken);
    }
    catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchKey")
    {
      logger_.LogError("The key {key} was not found.",
                       key);
      throw new ObjectDataNotFoundException("Key not found");
    }

    // Get the data from the response stream
    using var reader      = new StreamReader(response.ResponseStream);
    var       fileContent = await reader.ReadToEndAsync();

    var valuesCount = int.Parse(fileContent);

    if (valuesCount == 0)
    {
      yield break;
    }

    foreach (var chunkTask in Enumerable.Range(0,
                                               valuesCount)
                                        .Select(index => s3Client_.StringByteGetAsync(bucketName_,
                                                                                      $"{objectStorageName_}{key}_{index}",
                                                                                      logger_))
                                        .ToList())
    {
      yield return (await chunkTask.ConfigureAwait(false))!;
    }
  }

  /// <inheritdoc />
  public async Task<bool> TryDeleteAsync(string            key,
                                         CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    var value = await s3Client_.StringGetValueAsync(bucketName_,
                                                    $"{objectStorageName_}{key}_count",
                                                    logger_)
                               .ConfigureAwait(false);

    if (value == null)
    {
      throw new ObjectDataNotFoundException("Key not found");
    }

    var valuesCount = int.Parse(value!);
    var keyList = Enumerable.Range(0,
                                   valuesCount)
                            .Select(index => new KeyVersion
                                             {
                                               Key = $"{objectStorageName_}{key}_{index}",
                                             })
                            .Concat(new[]
                                    {
                                      new KeyVersion
                                      {
                                        Key = $"{objectStorageName_}{key}_count",
                                      },
                                    });

    var deletedItem = 0;
    try
    {
      await keyList.Chunk(MaxAllowedKeysPerDelete)
                   .Select(async chunkedKeyList =>
                           {
                             var multiObjectDeleteRequest = new DeleteObjectsRequest
                                                            {
                                                              BucketName = bucketName_,
                                                              Objects    = chunkedKeyList.ToList(),
                                                            };
                             var deleteObjectsResponse = await s3Client_.DeleteObjectsAsync(multiObjectDeleteRequest,
                                                                                            cancellationToken)
                                                                        .ConfigureAwait(false);
                             Interlocked.Add(ref deletedItem,
                                             deleteObjectsResponse.DeletedObjects.Count);
                           })
                   .WhenAll();
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Error deleting S3 bucket : {bucketName}",
                       bucketName_);
    }

    return deletedItem == valuesCount + 1;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();
}

internal static class AmazonS3ClientExt
{
  internal static async Task<PutObjectResponse> WriteObjectAsync(this AmazonS3Client  s3Client,
                                                                 string               bucketName,
                                                                 string               key,
                                                                 ReadOnlyMemory<byte> chunk)
  {
    var request = new PutObjectRequest
                  {
                    BucketName  = bucketName,
                    Key         = key,
                    InputStream = chunk.AsStream(),
                  };
    return await s3Client.PutObjectAsync(request);
  }

  internal static Task<PutObjectResponse> WriteStringAsync(this AmazonS3Client s3Client,
                                                           string              bucketName,
                                                           string              key,
                                                           string              dataString)
  {
    var requestCount = new PutObjectRequest
                       {
                         BucketName  = bucketName,
                         Key         = key,
                         ContentBody = dataString,
                       };
    return s3Client.PutObjectAsync(requestCount);
  }

  internal static async Task<byte[]> StringByteGetAsync(this AmazonS3Client    s3Client,
                                                        string                 bucketName,
                                                        string                 key,
                                                        ILogger<ObjectStorage> logger)
  {
    var request = new GetObjectRequest
                  {
                    BucketName = bucketName,
                    Key        = key,
                  };
    using var response     = await s3Client.GetObjectAsync(request);
    using var memoryStream = new MemoryStream();
    await response.ResponseStream.CopyToAsync(memoryStream);
    var fileContent = memoryStream.ToArray();

    return fileContent;
  }

  internal static async Task<string?> StringGetValueAsync(this AmazonS3Client    s3Client,
                                                          string                 bucketName,
                                                          string                 key,
                                                          ILogger<ObjectStorage> logger)
  {
    var response = await s3Client.GetObjectAsync(bucketName,
                                                 key);

    // Get the data from the response stream
    await using var responseStream = response.ResponseStream;

    var retrievedData = new byte[responseStream.Length];
    _ = await responseStream.ReadAsync(retrievedData,
                                       0,
                                       retrievedData.Length);
    return Encoding.UTF8.GetString(retrievedData);
  }
}
