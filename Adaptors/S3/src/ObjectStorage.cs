// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Util;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.S3;

public class ObjectStorage : IObjectStorage
{
  private const    int                    MaxAllowedKeysPerDelete = 1000;
  private readonly string                 bucketName_;
  private readonly int                    chunkDownloadSize_;
  private readonly int                    degreeOfParallelism_;
  private readonly ILogger<ObjectStorage> logger_;
  private readonly string                 objectStorageName_;
  private readonly AmazonS3Client         s3Client_;
  private          bool                   isInitialized_;

  /// <summary>
  ///   <see cref="IObjectStorage" /> implementation for S3
  /// </summary>
  /// <param name="s3Client">Connection to S3</param>
  /// <param name="objectStorageName">Name of the object storage used to differentiate them</param>
  /// <param name="options">S3 object storage options</param>
  /// <param name="logger">Logger used to print logs</param>
  public ObjectStorage(AmazonS3Client         s3Client,
                       Options.S3             options,
                       ILogger<ObjectStorage> logger)
  {
    s3Client_            = s3Client;
    objectStorageName_   = "objectStorageName";
    bucketName_          = options.BucketName;
    chunkDownloadSize_   = options.ChunkDownloadSize;
    logger_              = logger;
    degreeOfParallelism_ = options.DegreeOfParallelism;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await AmazonS3Util.DoesS3BucketExistV2Async(s3Client_,
                                                  bucketName_);
    }

    logger_.LogInformation("ObjectStorageFactory has correctly been initialized.");
    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    switch (tag)
    {
      case HealthCheckTag.Startup:
      case HealthCheckTag.Readiness:
        return Task.FromResult(isInitialized_
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("S3 not initialized yet."));
      case HealthCheckTag.Liveness:
        return Task.FromResult(isInitialized_
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("S3 not initialized or connection dropped."));
      default:
        throw new ArgumentOutOfRangeException(nameof(tag),
                                              tag,
                                              null);
    }
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
                                                $"{objectStorageName_}{key}",
                                                cancellationToken);
    }
    catch (AmazonS3Exception ex) when (ex.ErrorCode == "NoSuchKey")
    {
      logger_.LogError("The key {key} was not found.",
                       key);
      throw new ObjectDataNotFoundException("Key not found");
    }

    var metaDataRequest = new GetObjectMetadataRequest
                          {
                            Key        = $"{objectStorageName_}{key}",
                            BucketName = bucketName_,
                          };
    var objectMetaData = await s3Client_.GetObjectMetadataAsync(metaDataRequest,
                                                                cancellationToken);
    var contentLength = objectMetaData.ContentLength;
    var nbChunks      = (contentLength + chunkDownloadSize_ - 1) / chunkDownloadSize_;

    var getObjectRequest = new GetObjectRequest
                           {
                             BucketName = bucketName_,
                             Key        = $"{objectStorageName_}{key}",
                           };
    var objectResponse = await s3Client_.GetObjectAsync(getObjectRequest,
                                                        cancellationToken);
    var  responseStream = objectResponse.ResponseStream;
    long totalBytesRead = 0;
    while (totalBytesRead < contentLength)
    {
      var downloadedChunkSize = Math.Min(contentLength - totalBytesRead,
                                         chunkDownloadSize_);
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
  public async Task<bool> TryDeleteAsync(string            key,
                                         CancellationToken cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    try
    {
      var objectDeleteRequest = new DeleteObjectRequest
                                {
                                  BucketName = bucketName_,
                                  Key        = $"{objectStorageName_}{key}",
                                };
      var deleteObjectResponse = await s3Client_.DeleteObjectAsync(objectDeleteRequest,
                                                                   cancellationToken)
                                                .ConfigureAwait(false);
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "Error deleting S3 bucket : {bucketName}",
                       bucketName_);
    }

    return true;
  }

  /// <inheritdoc />
  public IAsyncEnumerable<string> ListKeysAsync(CancellationToken cancellationToken = default)
    => throw new NotImplementedException();

  /// <inheritdoc />
  public async Task AddOrUpdateAsync(string                                 key,
                                     IAsyncEnumerable<ReadOnlyMemory<byte>> valueChunks,
                                     CancellationToken                      cancellationToken = default)
  {
    using var _ = logger_.LogFunction(objectStorageName_ + key);
    var initRequest = new InitiateMultipartUploadRequest
                      {
                        BucketName = bucketName_,
                        Key        = $"{objectStorageName_}{key}",
                      };
    var initResponse = await s3Client_.InitiateMultipartUploadAsync(initRequest,
                                                                    cancellationToken)
                                      .ConfigureAwait(false);
    try
    {
      var uploadRequests = await UploadMultiPartHelper.PreparePartRequestsAsync(bucketName_,
                                                                                $"{objectStorageName_}{key}",
                                                                                initResponse.UploadId,
                                                                                valueChunks,
                                                                                cancellationToken)
                                                      .ConfigureAwait(false);

      var partETags = await uploadRequests.ParallelSelect(new ParallelTaskOptions(degreeOfParallelism_),
                                                          async uploadPartRequest =>
                                                          {
                                                            var uploadPartResponse = await s3Client_.UploadPartAsync(uploadPartRequest,
                                                                                                                     cancellationToken)
                                                                                                    .ConfigureAwait(false);
                                                            var partETag = new PartETag
                                                                           {
                                                                             ETag       = uploadPartResponse.ETag,
                                                                             PartNumber = uploadPartResponse.PartNumber,
                                                                           };
                                                            return partETag;
                                                          })
                                          .ToListAsync(cancellationToken)
                                          .ConfigureAwait(false);

      var compRequest = new CompleteMultipartUploadRequest
                        {
                          BucketName = bucketName_,
                          Key        = $"{objectStorageName_}{key}",
                          UploadId   = initResponse.UploadId,
                          PartETags  = partETags,
                        };

      var compResponse = await s3Client_.CompleteMultipartUploadAsync(compRequest,
                                                                      cancellationToken)
                                        .ConfigureAwait(false);
    }
    catch (Exception e)
    {
      logger_.LogError(e,
                       "Multipart upload is being aborted");
      var abortMpuRequest = new AbortMultipartUploadRequest
                            {
                              BucketName = bucketName_,
                              Key        = $"{objectStorageName_}{key}",
                              UploadId   = initResponse.UploadId,
                            };
      await s3Client_.AbortMultipartUploadAsync(abortMpuRequest,
                                                cancellationToken);
    }
  }
}
