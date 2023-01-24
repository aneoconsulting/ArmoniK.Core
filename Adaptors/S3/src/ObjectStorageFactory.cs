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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using Amazon.S3;
using Amazon.S3.Util;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.S3;

public class ObjectStorageFactory : IObjectStorageFactory
{
  private readonly ILoggerFactory loggerFactory_;
  private readonly Options.S3     options_;
  private readonly AmazonS3Client s3Client_;
  private          bool           isInitialized_;

  public ObjectStorageFactory(AmazonS3Client s3Client,
                              Options.S3     options,
                              ILoggerFactory loggerFactory)
  {
    s3Client_      = s3Client;
    options_       = options;
    loggerFactory_ = loggerFactory;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    var logger = loggerFactory_.CreateLogger<ObjectStorage>();
    if (!isInitialized_)
    {
      await AmazonS3Util.DoesS3BucketExistV2Async(s3Client_,
                                                  options_.BucketName);
    }

    logger.LogInformation("ObjectStorageFactory has correctly been initialized.");
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

  public IObjectStorage CreateObjectStorage(string objectStorageName)
    => new ObjectStorage(s3Client_,
                         objectStorageName,
                         options_,
                         loggerFactory_.CreateLogger<ObjectStorage>());
}
