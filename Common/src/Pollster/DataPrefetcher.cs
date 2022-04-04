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

using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Pollster;

public class DataPrefetcher : IInitializable
{
  private readonly ActivitySource          activitySource_;
  private readonly IObjectStorageFactory   objectStorageFactory_;
  private readonly ILogger<DataPrefetcher> logger_;

  public DataPrefetcher(
    IObjectStorageFactory   objectStorageFactory,
    ActivitySource          activitySource,
    ILogger<DataPrefetcher> logger)
  {
    objectStorageFactory_ = objectStorageFactory;
    logger_               = logger;
    activitySource_       = activitySource;
  }

  public async Task<Queue<ProcessRequest.Types.ComputeRequest>> PrefetchDataAsync(TaskData taskData, CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity(nameof(PrefetchDataAsync));

    var resultStorage  = objectStorageFactory_.CreateResultStorage(taskData.SessionId);
    var payloadStorage = objectStorageFactory_.CreatePayloadStorage(taskData.SessionId);

    List<ByteString> payloadChunks;

    activity?.AddEvent(new("Load payload"));

    if (taskData.HasPayload)
    {
      payloadChunks = new()
                      {
                        UnsafeByteOperations.UnsafeWrap(taskData.Payload),
                      };
    }
    else
    {
      payloadChunks = await payloadStorage.GetValuesAsync(taskData.TaskId,
                                                             cancellationToken)
                                          .Select(bytes => UnsafeByteOperations.UnsafeWrap(bytes))
                                          .ToListAsync(cancellationToken);
    }

    var computeRequests = new ComputeRequestQueue(logger_);
    computeRequests.Init(PayloadConfiguration.MaxChunkSize, taskData.SessionId, taskData.TaskId, taskData.Options.Options, payloadChunks.FirstOrDefault(), taskData.ExpectedOutput);

    for (var i = 1; i < payloadChunks.Count; i++)
    {
      computeRequests.AddPayloadChunk(payloadChunks[i]);
    }
    computeRequests.CompletePayload();

    foreach (var dataDependency in taskData.DataDependencies)
    {
      var dependencyChunks = await resultStorage.GetValuesAsync(dataDependency,
                                                                   cancellationToken)
                                                .Select(bytes => UnsafeByteOperations.UnsafeWrap(bytes))
                                                .ToListAsync(cancellationToken);

      computeRequests.InitDataDependency(dataDependency);
      foreach (var chunk in dependencyChunks)
      {
        computeRequests.AddDataDependencyChunk(chunk);
      }
      computeRequests.CompleteDataDependency();
    }

    return computeRequests.GetQueue();
  }

  private bool isInitialized_ = false;

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag) => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      await objectStorageFactory_.Init(cancellationToken);
      isInitialized_ = true;
    }
  }
}
