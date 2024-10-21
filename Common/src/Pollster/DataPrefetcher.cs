// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Prefetch data needed to execute a task
/// </summary>
public class DataPrefetcher : IInitializable
{
  private readonly ActivitySource?         activitySource_;
  private readonly ILogger<DataPrefetcher> logger_;
  private readonly IObjectStorage          objectStorage_;

  private bool isInitialized_;

  /// <summary>
  ///   Create data prefetcher for tasks
  /// </summary>
  /// <param name="objectStorage">Interface to manage data</param>
  /// <param name="activitySource">Activity source for tracing</param>
  /// <param name="logger">Logger used to print logs</param>
  public DataPrefetcher(IObjectStorage          objectStorage,
                        ActivitySource?         activitySource,
                        ILogger<DataPrefetcher> logger)
  {
    objectStorage_  = objectStorage;
    logger_         = logger;
    activitySource_ = activitySource;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await objectStorage_.Init(cancellationToken)
                          .ConfigureAwait(false);
      isInitialized_ = true;
    }
  }

  /// <summary>
  ///   Method used to prefetch data before executing a task
  /// </summary>
  /// <param name="taskData">Task metadata</param>
  /// <param name="folder">Path in which pre-fetched data are stored</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">input data are not found</exception>
  public async Task PrefetchDataAsync(TaskData          taskData,
                                      string            folder,
                                      CancellationToken cancellationToken)
  {
    using var activity     = activitySource_?.StartActivity();
    using var sessionScope = logger_.BeginPropertyScope(("sessionId", taskData.SessionId));

    activity?.AddEvent(new ActivityEvent("Load payload"));


    await using (var fs = new FileStream(Path.Combine(folder,
                                                      taskData.PayloadId),
                                         FileMode.OpenOrCreate))
    {
      await using var w = new BinaryWriter(fs);
      await foreach (var chunk in objectStorage_.GetValuesAsync(taskData.PayloadId,
                                                                cancellationToken)
                                                .ConfigureAwait(false))
      {
        w.Write(chunk);
      }
    }


    foreach (var dataDependency in taskData.DataDependencies)
    {
      await using var fs = new FileStream(Path.Combine(folder,
                                                       dataDependency),
                                          FileMode.OpenOrCreate);
      await using var w = new BinaryWriter(fs);
      await foreach (var chunk in objectStorage_.GetValuesAsync(dataDependency,
                                                                cancellationToken)
                                                .ConfigureAwait(false))
      {
        w.Write(chunk);
      }
    }
  }
}
