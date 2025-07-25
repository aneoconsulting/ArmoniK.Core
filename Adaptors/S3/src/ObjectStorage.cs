// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.S3;

public class ObjectStorage : IObjectStorage
{
  private const    long                   MinPartSize  = 5 * 1024 * 1024; // 5 MiB
  private const    int                    MaxPartCount = 10000;
  private const    long                   MaxPartSize  = 5L * 1024 * 1024 * 1024; // 5 GiB
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly Options.S3             options_;
  private readonly AmazonS3Client         s3Client_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for S3
  /// </summary>
  /// <param name="s3Client">Connection to S3</param>
  /// <param name="options">S3 object storage options</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(AmazonS3Client         s3Client,
                       Options.S3             options,
                       ILogger<ObjectStorage> logger)
  {
    s3Client_          = s3Client;
    objectStorageName_ = "objectStorageName";
    options_           = options;
    logger_            = logger;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await AmazonS3Util.DoesS3BucketExistV2Async(s3Client_,
                                                  options_.BucketName)
                        .ConfigureAwait(false);
    }

    logger_.LogInformation("ObjectStorage has correctly been initialized with options {@Options}",
                           options_.Confidential());
    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => tag switch
       {
         HealthCheckTag.Startup or HealthCheckTag.Readiness => Task.FromResult(isInitialized_
                                                                                 ? HealthCheckResult.Healthy()
                                                                                 : HealthCheckResult.Unhealthy("S3 not initialized yet.")),
         HealthCheckTag.Liveness => Task.FromResult(isInitialized_
                                                      ? HealthCheckResult.Healthy()
                                                      : HealthCheckResult.Unhealthy("S3 not initialized or connection dropped.")),
         _ => throw new ArgumentOutOfRangeException(nameof(tag),
                                                    tag,
                                                    null),
       };

  /// <inheritdoc />
  public async IAsyncEnumerable<byte[]> GetValuesAsync(byte[]                                     id,
                                                       [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var key                   = Encoding.UTF8.GetString(id);
    var objectStorageFullName = $"{objectStorageName_}{key}";

    using var objectResponse = await GetObjectStream(objectStorageFullName,
                                                     cancellationToken)
                                 .ConfigureAwait(false);

    var  contentLength  = objectResponse.ContentLength;
    var  responseStream = objectResponse.ResponseStream;
    long totalBytesRead = 0;
    while (totalBytesRead < contentLength)
    {
      var downloadedChunkSize = Math.Min(contentLength - totalBytesRead,
                                         options_.ChunkDownloadSize);
      var downloadedChunk = new byte[downloadedChunkSize];
      var bytesRead = await responseStream.ReadAsync(downloadedChunk,
                                                     cancellationToken)
                                          .ConfigureAwait(false);
      while (bytesRead != downloadedChunkSize)
      {
        var remainingBytesRead = await responseStream.ReadAsync(downloadedChunk,
                                                                bytesRead,
                                                                (int)(downloadedChunkSize - bytesRead),
                                                                cancellationToken)
                                                     .ConfigureAwait(false);
        if (remainingBytesRead == 0)
        {
          throw new ArmoniKException("S3 Partial Read");
        }

        bytesRead += remainingBytesRead;
      }

      totalBytesRead += bytesRead;
      yield return downloadedChunk;
    }
  }

  /// <inheritdoc />
  public async Task<(byte[] id, long size)> AddOrUpdateAsync(ObjectData                             metaData,
                                                             IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                                             CancellationToken                      cancellationToken = default)

  {
    long[] sizeBox =
    {
      0,
    };
    var key = Guid.NewGuid()
                  .ToString();
    var objectStorageFullName = $"{objectStorageName_}{key}";

    logger_.LogDebug("Upload object");
    var initRequest = new InitiateMultipartUploadRequest
                      {
                        BucketName = options_.BucketName,
                        Key        = objectStorageFullName,
                      };
    var initResponse = await s3Client_.InitiateMultipartUploadAsync(initRequest,
                                                                    cancellationToken)
                                      .ConfigureAwait(false);
    try
    {
      var uploadRequest = PreparePartRequestsAsync(objectStorageFullName,
                                                   initResponse.UploadId,
                                                   valueChunks,
                                                   sizeBox,
                                                   cancellationToken);
      var uploadResponses = await uploadRequest.ParallelSelect(new ParallelTaskOptions(options_.DegreeOfParallelism),
                                                               async uploadPartRequest => await s3Client_.UploadPartAsync(uploadPartRequest,
                                                                                                                          cancellationToken)
                                                                                                         .ConfigureAwait(false))
                                               .ToListAsync(cancellationToken)
                                               .ConfigureAwait(false);

      var compRequest = new CompleteMultipartUploadRequest
                        {
                          BucketName = options_.BucketName,
                          Key        = objectStorageFullName,
                          UploadId   = initResponse.UploadId,
                        };
      compRequest.AddPartETags(uploadResponses);
      await s3Client_.CompleteMultipartUploadAsync(compRequest,
                                                   cancellationToken)
                     .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Multipart upload is being aborted");
      var abortMpuRequest = new AbortMultipartUploadRequest
                            {
                              BucketName = options_.BucketName,
                              Key        = objectStorageFullName,
                              UploadId   = initResponse.UploadId,
                            };
      await s3Client_.AbortMultipartUploadAsync(abortMpuRequest,
                                                cancellationToken)
                     .ConfigureAwait(false);
      throw;
    }

    return (Encoding.UTF8.GetBytes(key), sizeBox[0]);
  }

  /// <inheritdoc />
  public async Task TryDeleteAsync(IEnumerable<byte[]> ids,
                                   CancellationToken   cancellationToken = default)
    => await ids.ParallelForEach(id => TryDeleteAsync(id,
                                                      cancellationToken))
                .ConfigureAwait(false);

  /// <inheritdoc />
  public async Task<IDictionary<byte[], long?>> GetSizesAsync(IEnumerable<byte[]> ids,
                                                              CancellationToken   cancellationToken = default)
    => await ids.ParallelSelect(async id => (id, await GetSizeAsync(id,
                                                                    cancellationToken)
                                                   .ConfigureAwait(false)))
                .ToDictionaryAsync(tuple => tuple.id,
                                   tuple => tuple.Item2,
                                   cancellationToken)
                .ConfigureAwait(false);

  private async Task<GetObjectResponse> GetObjectStream(string            key,
                                                        CancellationToken cancellationToken)
  {
    var getObjectRequest = new GetObjectRequest
                           {
                             BucketName = options_.BucketName,
                             Key        = key,
                           };

    for (var retryCount = 0; retryCount < options_.MaxRetry; retryCount++)
    {
      try
      {
        return await s3Client_.GetObjectAsync(getObjectRequest,
                                              cancellationToken)
                              .ConfigureAwait(false);
      }
      catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchKey")
      {
        logger_.LogError("The key {Key} was not found",
                         key);
        throw new ObjectDataNotFoundException("Key not found");
      }
      catch (AmazonS3Exception ex)
      {
        if (retryCount + 1 >= options_.MaxRetry)
        {
          logger_.LogError(ex,
                           "A AmazonS3Exception occurred {RetryCount} times for the same action",
                           options_.MaxRetry);
          throw;
        }

        var retryDelay = (retryCount + 1) * (retryCount + 1) * options_.MsAfterRetry;
        logger_.LogWarning(ex,
                           "A AmazonS3Exception occurred {RetryCount}/{MaxRetry}, retry in {RetryDelay} ms",
                           retryCount,
                           options_.MaxRetry,
                           retryDelay);
        await Task.Delay(retryDelay,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }

    throw new AmazonS3Exception("A AmazonS3Exception occurred");
  }

  private async Task<long?> GetSizeAsync(byte[]            id,
                                         CancellationToken cancellationToken)
  {
    try
    {
      var key                   = Encoding.UTF8.GetString(id);
      var objectStorageFullName = $"{objectStorageName_}{key}";

      var metadata = await s3Client_.GetObjectMetadataAsync(new GetObjectMetadataRequest
                                                            {
                                                              BucketName = options_.BucketName,
                                                              Key        = objectStorageFullName,
                                                            },
                                                            cancellationToken)
                                    .ConfigureAwait(false);

      return metadata.ContentLength;
    }
    catch (AmazonS3Exception e) when (e.StatusCode == HttpStatusCode.NotFound)
    {
      return null;
    }
  }

  private async Task TryDeleteAsync(byte[]            id,
                                    CancellationToken cancellationToken = default)
  {
    var key                   = Encoding.UTF8.GetString(id);
    var objectStorageFullName = $"{objectStorageName_}{key}";

    var objectDeleteRequest = new DeleteObjectRequest
                              {
                                BucketName = options_.BucketName,
                                Key        = objectStorageFullName,
                              };
    await s3Client_.DeleteObjectAsync(objectDeleteRequest,
                                      cancellationToken)
                   .ConfigureAwait(false);
    logger_.LogInformation("Deleted data with {resultId}",
                           key);
  }

  private async IAsyncEnumerable<UploadPartRequest> PreparePartRequestsAsync(string                                     objectKey,
                                                                             string                                     uploadId,
                                                                             IAsyncEnumerable<ReadOnlyMemory<byte>>     valueChunks,
                                                                             long[]                                     sizeBox,
                                                                             [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    long size              = 0;
    var  partNumber        = 1;
    long bytesRead         = 0;
    var  currentPartSize   = MinPartSize;
    var  currentPartStream = new MemoryStream();

    await foreach (var chunk in valueChunks.WithCancellation(cancellationToken)
                                           .ConfigureAwait(false))
    {
      long chunkSize = chunk.Length;
      size      += chunkSize;
      bytesRead += chunkSize;
      var remainingStream = new MemoryStream();

      if (bytesRead <= MaxPartSize)
      {
        await currentPartStream.WriteAsync(chunk.ToArray(),
                                           cancellationToken)
                               .ConfigureAwait(false);
      }
      else
      {
        var plusSize = bytesRead - MaxPartSize;
        var chunkSlice = chunk.Slice(0,
                                     (int)(chunkSize - plusSize));
        var remainingSlice = chunk.Slice((int)(chunkSize - plusSize),
                                         (int)plusSize);
        await currentPartStream.WriteAsync(chunkSlice.ToArray(),
                                           cancellationToken)
                               .ConfigureAwait(false);
        await remainingStream.WriteAsync(remainingSlice.ToArray(),
                                         cancellationToken)
                             .ConfigureAwait(false);
      }

      if (bytesRead >= currentPartSize && partNumber < MaxPartCount)
      {
        var partRequest = new UploadPartRequest
                          {
                            BucketName                       = options_.BucketName,
                            Key                              = objectKey,
                            PartNumber                       = partNumber,
                            InputStream                      = new MemoryStream(currentPartStream.ToArray()),
                            UploadId                         = uploadId,
                            UseChunkEncoding                 = options_.UseChunkEncoding,
                            DisableDefaultChecksumValidation = !options_.UseChecksum,
                          };
        yield return partRequest;
        currentPartStream = new MemoryStream();
        await remainingStream.CopyToAsync(currentPartStream,
                                          cancellationToken)
                             .ConfigureAwait(false);
        bytesRead = remainingStream.Length;
        partNumber++;
        currentPartSize += 1024 * 1024; // 1MiB
        if (currentPartSize > MaxPartSize)
        {
          currentPartSize = MaxPartSize;
        }
      }
    }

    /* This "may be" the last part, in one of these cases:
     Either we have a remainder part of size < MinPartSize
     Or we have reached the limit of the maximum number of parts and we still have data. We have already prepared MaxPartCount - 1 = 9999, the remaining data is accumulated and sent in the last(the 10000th) part, there is no guarantee here that it is < 5GiB
    */
    if (currentPartStream.Length >= 0)
    {
      var partRequest = new UploadPartRequest
                        {
                          BucketName                       = options_.BucketName,
                          Key                              = objectKey,
                          PartNumber                       = partNumber,
                          InputStream                      = new MemoryStream(currentPartStream.ToArray()),
                          UploadId                         = uploadId,
                          UseChunkEncoding                 = options_.UseChunkEncoding,
                          DisableDefaultChecksumValidation = !options_.UseChecksum,
                        };
      yield return partRequest;
    }

    // It is not possible to pass a ref parameter to an Iterator function
    // So boxing is required to pass the value back to the caller after the enumeration
    sizeBox[0] = size;
  }
}
