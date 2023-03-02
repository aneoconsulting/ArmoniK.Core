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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Object;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.MongoDB;

public class ObjectStorageFactory : IObjectStorageFactory
{
  private readonly ILoggerFactory                                                          loggerFactory_;
  private readonly MongoCollectionProvider<ObjectDataModelMapping, ObjectDataModelMapping> objectCollectionProvider_;
  private readonly Options.ObjectStorage                                                   options_;
  private readonly SessionProvider                                                         sessionProvider_;


  private bool isInitialized_;

  public ObjectStorageFactory(ILoggerFactory                                                          loggerFactory,
                              SessionProvider                                                         sessionProvider,
                              MongoCollectionProvider<ObjectDataModelMapping, ObjectDataModelMapping> objectCollectionProvider,
                              Options.ObjectStorage                                                   options)
  {
    loggerFactory_            = loggerFactory;
    sessionProvider_          = sessionProvider;
    objectCollectionProvider_ = objectCollectionProvider;
    options_                  = options;
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await sessionProvider_.Init(cancellationToken)
                      .ConfigureAwait(false);
      sessionProvider_.Get();

      await objectCollectionProvider_.Init(cancellationToken)
                                     .ConfigureAwait(false);
      objectCollectionProvider_.Get();
    }

    isInitialized_ = true;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  public IObjectStorage CreateObjectStorage(string objectStorageName)
    => new ObjectStorage(sessionProvider_,
                         objectCollectionProvider_,
                         objectStorageName,
                         loggerFactory_.CreateLogger<ObjectStorage>(),
                         options_);
}
