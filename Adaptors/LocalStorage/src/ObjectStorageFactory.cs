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

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.LocalStorage;

public class ObjectStorageFactory : IObjectStorageFactory
{
  private readonly int            chunkSize_;
  private readonly ILogger        logger_;
  private readonly ILoggerFactory loggerFactory_;
  private readonly string         rootPath_;


  private bool isInitialized_;

  public ObjectStorageFactory(string         rootPath,
                              int            chunkSize,
                              ILoggerFactory loggerFactory)
  {
    rootPath_ = rootPath == ""
                  ? Options.LocalStorage.Default.Path
                  : rootPath;
    chunkSize_ = chunkSize == 0
                   ? Options.LocalStorage.Default.ChunkSize
                   : chunkSize;
    loggerFactory_ = loggerFactory;
    logger_        = loggerFactory.CreateLogger<ObjectStorageFactory>();

    logger_.LogDebug("Creating Local ObjectStorageFactory at path {path}, chunked by {chunkSize}",
                     rootPath_,
                     chunkSize_);
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    _ = cancellationToken;
    logger_.LogDebug("Initializing Local ObjectStorageFactory at path {path}, chunked by {chunkSize}",
                     rootPath_,
                     chunkSize_);
    Directory.CreateDirectory(rootPath_);
    isInitialized_ = true;
    return Task.CompletedTask;
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
                                 : HealthCheckResult.Unhealthy("Local storage not initialized yet."));
      case HealthCheckTag.Liveness:
        return Task.FromResult(isInitialized_ && Directory.Exists(rootPath_)
                                 ? HealthCheckResult.Healthy()
                                 : HealthCheckResult.Unhealthy("Local storage not initialized or folder has been deleted."));
      default:
        throw new ArgumentOutOfRangeException(nameof(tag),
                                              tag,
                                              null);
    }
  }

  public IObjectStorage CreateObjectStorage(string objectStorageName)
    => new ObjectStorage(Path.Combine(rootPath_,
                                      objectStorageName),
                         chunkSize_,
                         loggerFactory_.CreateLogger<ObjectStorage>());
}
