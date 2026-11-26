// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;

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
  private readonly int                     parallelism_;

  private bool isInitialized_;

  /// <summary>
  ///   Create data prefetcher for tasks
  /// </summary>
  /// <param name="objectStorage">Interface to manage data</param>
  /// <param name="pollsterOptions">Pollster configuration</param>
  /// <param name="activitySource">Activity source for tracing</param>
  /// <param name="logger">Logger used to print logs</param>
  public DataPrefetcher(IObjectStorage             objectStorage,
                        Injection.Options.Pollster pollsterOptions,
                        ActivitySource?            activitySource,
                        ILogger<DataPrefetcher>    logger)
  {
    objectStorage_  = objectStorage;
    parallelism_    = pollsterOptions.DataPrefetchParallelism;
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
  ///   Method used to prefetch data before executing a task.
  ///   The data are downloaded concurrently, up to <see cref="Injection.Options.Pollster.DataPrefetchParallelism" />
  ///   downloads at a time.
  /// </summary>
  /// <param name="opaqueIds">Dictionary containing </param>
  /// <param name="folder">Path in which pre-fetched data are stored</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   Task representing the asynchronous execution of the method
  /// </returns>
  /// <exception cref="ObjectDataNotFoundException">input data are not found</exception>
  public async Task PrefetchDataAsync(Dictionary<string, byte[]> opaqueIds,
                                      string                     folder,
                                      CancellationToken          cancellationToken)
  {
    using var activity     = activitySource_?.StartActivity();
    using var sessionScope = logger_.BeginPropertyScope(("results", opaqueIds));

    activity?.AddEvent(new ActivityEvent("Load payload"));


    // Unlike ParallelForEach, a failure cancels the other downloads and waits for them, so none of them keeps
    // writing into the task folder once the error is handled.
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    using var throttle = parallelism_ < 0
                           ? null
                           : new SemaphoreSlim(parallelism_ == 0
                                                 ? Environment.ProcessorCount
                                                 : parallelism_);
    Exception? failure = null;

    var downloads = opaqueIds.Select(async pair =>
                                     {
                                       try
                                       {
                                         if (throttle is not null)
                                         {
                                           await throttle.WaitAsync(cts.Token)
                                                         .ConfigureAwait(false);
                                         }

                                         try
                                         {
                                           // A slot released by a cancelled download can be granted before the
                                           // cancellation reaches this waiter
                                           cts.Token.ThrowIfCancellationRequested();
                                           await DownloadAsync(pair.Value,
                                                               Path.Combine(folder,
                                                                            pair.Key),
                                                               cts.Token)
                                             .ConfigureAwait(false);
                                         }
                                         finally
                                         {
                                           throttle?.Release();
                                         }
                                       }
                                       catch (Exception e)
                                       {
                                         Interlocked.CompareExchange(ref failure,
                                                                     e,
                                                                     null);
                                         await cts.CancelAsync()
                                                  .ConfigureAwait(false);
                                         throw;
                                       }
                                     })
                             .ToList();

    try
    {
      await Task.WhenAll(downloads)
                .ConfigureAwait(false);
    }
    catch when (failure is not null)
    {
      // Rethrow the root cause rather than the cancellation it triggered in another download
      ExceptionDispatchInfo.Throw(failure);
    }
  }

  private async Task DownloadAsync(byte[]            opaqueId,
                                   string            path,
                                   CancellationToken cancellationToken)
  {
    await using var fs = new FileStream(path,
                                        FileMode.Create,
                                        FileAccess.Write,
                                        FileShare.None,
                                        4096,
                                        FileOptions.Asynchronous);
    await foreach (var chunk in objectStorage_.GetValuesAsync(opaqueId,
                                                              cancellationToken)
                                              .ConfigureAwait(false))
    {
      await fs.WriteAsync(chunk,
                          cancellationToken)
              .ConfigureAwait(false);
    }
  }
}
