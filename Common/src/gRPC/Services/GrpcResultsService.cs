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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Convertors;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcResultsService : Results.ResultsBase
{
  private readonly ILogger<GrpcResultsService>                  logger_;
  private readonly FunctionExecutionMetrics<GrpcResultsService> meter_;
  private readonly IObjectStorage                               objectStorage_;
  private readonly Injection.Options.Submitter                  options_;
  private readonly IPushQueueStorage                            pushQueueStorage_;
  private readonly IResultTable                                 resultTable_;
  private readonly ISessionTable                                sessionTable_;
  private readonly ITaskTable                                   taskTable_;

  public GrpcResultsService(IResultTable                                 resultTable,
                            ITaskTable                                   taskTable,
                            ISessionTable                                sessionTable,
                            IObjectStorage                               objectStorage,
                            IPushQueueStorage                            pushQueueStorage,
                            FunctionExecutionMetrics<GrpcResultsService> meter,
                            Injection.Options.Submitter                  options,
                            ILogger<GrpcResultsService>                  logger)
  {
    logger_           = logger;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    sessionTable_     = sessionTable;
    objectStorage_    = objectStorage;
    pushQueueStorage_ = pushQueueStorage;
    meter_            = meter;
    options_          = options;
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(GetOwnerTaskId))]
  public override async Task<GetOwnerTaskIdResponse> GetOwnerTaskId(GetOwnerTaskIdRequest request,
                                                                    ServerCallContext     context)
  {
    using var measure = meter_.CountAndTime();
    using var _       = logger_.LogFunction();

    return new GetOwnerTaskIdResponse
           {
             SessionId = request.SessionId,
             ResultTask =
             {
               await resultTable_.GetResults(request.SessionId,
                                             request.ResultId,
                                             context.CancellationToken)
                                 .Select(result => new GetOwnerTaskIdResponse.Types.MapResultTask
                                                   {
                                                     TaskId   = result.OwnerTaskId,
                                                     ResultId = result.Name,
                                                   })
                                 .ToListAsync(context.CancellationToken)
                                 .ConfigureAwait(false),
             },
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(ListResults))]
  public override async Task<ListResultsResponse> ListResults(ListResultsRequest request,
                                                              ServerCallContext  context)
  {
    using var measure = meter_.CountAndTime();
    var results = await resultTable_.ListResultsAsync(request.Filters is null
                                                        ? data => true
                                                        : request.Filters.ToResultFilter(),
                                                      request.Sort is null
                                                        ? result => result.ResultId
                                                        : request.Sort.ToField(),
                                                      request.Sort is null || request.Sort.Direction == SortDirection.Asc,
                                                      request.Page,
                                                      request.PageSize,
                                                      context.CancellationToken)
                                    .ConfigureAwait(false);
    return new ListResultsResponse
           {
             PageSize = request.PageSize,
             Page     = request.Page,
             Results =
             {
               results.results.Select(result => result.ToGrpcResultRaw()),
             },
             Total = results.totalCount,
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(CreateResultsMetaData))]
  public override async Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                                  ServerCallContext            context)
  {
    using var measure = meter_.CountAndTime();
    var results = request.Results.Select(rc => new Result(request.SessionId,
                                                          Guid.NewGuid()
                                                              .ToString(),
                                                          rc.Name,
                                                          "",
                                                          "",
                                                          ResultStatus.Created,
                                                          new List<string>(),
                                                          DateTime.UtcNow,
                                                          0,
                                                          Array.Empty<byte>(),
                                                          rc.ManualDeletion))
                         .ToList();

    await resultTable_.Create(results,
                              context.CancellationToken)
                      .ConfigureAwait(false);

    return new CreateResultsMetaDataResponse
           {
             Results =
             {
               results.Select(result => result.ToGrpcResultRaw()),
             },
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(CreateResults))]
  public override async Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                                  ServerCallContext    context)
  {
    using var measure = meter_.CountAndTime();
    var results = await request.Results.ParallelSelect(new ParallelTaskOptions(options_.DegreeOfParallelism),
                                                       async rc =>
                                                       {
                                                         var resultId = Guid.NewGuid()
                                                                            .ToString();

                                                         var (id, size) = await objectStorage_.AddOrUpdateAsync(new ObjectData
                                                                                                                {
                                                                                                                  ResultId  = resultId,
                                                                                                                  SessionId = request.SessionId,
                                                                                                                },
                                                                                                                new List<ReadOnlyMemory<byte>>
                                                                                                                {
                                                                                                                  rc.Data.Memory,
                                                                                                                }.ToAsyncEnumerable(),
                                                                                                                context.CancellationToken)
                                                                                              .ConfigureAwait(false);

                                                         return (new Result(request.SessionId,
                                                                            resultId,
                                                                            rc.Name,
                                                                            "",
                                                                            request.SessionId,
                                                                            ResultStatus.Created,
                                                                            new List<string>(),
                                                                            DateTime.UtcNow,
                                                                            size,
                                                                            Array.Empty<byte>(),
                                                                            rc.ManualDeletion), id);
                                                       })
                               .ToListAsync()
                               .ConfigureAwait(false);

    await resultTable_.Create(results.ViewSelect(tuple => tuple.Item1),
                              context.CancellationToken)
                      .ConfigureAwait(false);

    await resultTable_.CompleteManyResults(results.Select(tuple => (tuple.Item1.ResultId, tuple.Item1.Size, tuple.id)),
                                           context.CancellationToken)
                      .ConfigureAwait(false);

    return new CreateResultsResponse
           {
             Results =
             {
               await resultTable_.GetResults(request.SessionId,
                                             results.Select(tuple => tuple.Item1.ResultId),
                                             context.CancellationToken)
                                 .Select(result => result.ToGrpcResultRaw())
                                 .ToListAsync()
                                 .ConfigureAwait(false),
             },
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(DeleteResultsData))]
  public override async Task<DeleteResultsDataResponse> DeleteResultsData(DeleteResultsDataRequest request,
                                                                          ServerCallContext        context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      await foreach (var ids in resultTable_.GetResults(result => request.ResultId.Contains(result.ResultId) && !result.ManualDeletion,
                                                        result => result.OpaqueId,
                                                        context.CancellationToken)
                                            .ToChunksAsync(500,
                                                           Timeout.InfiniteTimeSpan,
                                                           context.CancellationToken)
                                            .ConfigureAwait(false))
      {
        await objectStorage_.TryDeleteAsync(ids,
                                            context.CancellationToken)
                            .ConfigureAwait(false);
      }

      await resultTable_.UpdateManyResults(result => request.ResultId.Contains(result.ResultId) && !result.ManualDeletion,
                                           new UpdateDefinition<Result>().Set(result => result.Status,
                                                                              ResultStatus.DeletedData)
                                                                         .Set(result => result.OpaqueId,
                                                                              Array.Empty<byte>()),
                                           context.CancellationToken)
                        .ConfigureAwait(false);

      return new DeleteResultsDataResponse
             {
               ResultId =
               {
                 request.ResultId,
               },
             };
    }
    catch (ObjectDataNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while deleting results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result data not found"));
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while deleting results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result data not found"));
    }
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(DownloadResultData))]
  public override async Task DownloadResultData(DownloadResultDataRequest                       request,
                                                IServerStreamWriter<DownloadResultDataResponse> responseStream,
                                                ServerCallContext                               context)
  {
    using var measure = meter_.CountAndTime();
    try
    {
      var id = (await resultTable_.GetResult(request.ResultId)
                                  .ConfigureAwait(false)).OpaqueId;

      await foreach (var chunk in objectStorage_.GetValuesAsync(id,
                                                                context.CancellationToken)
                                                .ConfigureAwait(false))
      {
        await responseStream.WriteAsync(new DownloadResultDataResponse
                                        {
                                          DataChunk = UnsafeByteOperations.UnsafeWrap(chunk),
                                        })
                            .ConfigureAwait(false);
      }
    }
    catch (ObjectDataNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while downloading results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result data not found"));
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while downloading results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result data not found"));
    }
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(GetServiceConfiguration))]
  public override Task<ResultsServiceConfigurationResponse> GetServiceConfiguration(Empty             request,
                                                                                    ServerCallContext context)
  {
    using var measure = meter_.CountAndTime();
    return Task.FromResult(new ResultsServiceConfigurationResponse
                           {
                             DataChunkMaxSize = PayloadConfiguration.MaxChunkSize,
                           });
  }


  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(UploadResultData))]
  public override async Task<UploadResultDataResponse> UploadResultData(IAsyncStreamReader<UploadResultDataRequest> requestStream,
                                                                        ServerCallContext                           context)
  {
    using var measure = meter_.CountAndTime();
    if (!await requestStream.MoveNext(context.CancellationToken)
                            .ConfigureAwait(false))
    {
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        "Missing result metadata"),
                             "Missing result metadata");
    }

    var current = requestStream.Current;

    if (current.TypeCase != UploadResultDataRequest.TypeOneofCase.Id)
    {
      throw new RpcException(new Status(StatusCode.InvalidArgument,
                                        "Message should be an Id"),
                             "Message should be an Id");
    }

    var id = current.Id;
    var sessionTask = sessionTable_.GetSessionAsync(id.SessionId,
                                                    context.CancellationToken);


    var (opaqueId, size) = await objectStorage_.AddOrUpdateAsync(new ObjectData
                                                                 {
                                                                   ResultId  = id.ResultId,
                                                                   SessionId = id.SessionId,
                                                                 },
                                                                 requestStream.ReadAllAsync(context.CancellationToken)
                                                                              .Select(r => r.DataChunk.Memory),
                                                                 context.CancellationToken)
                                               .ConfigureAwait(false);

    try
    {
      var sessionData = await sessionTask.ConfigureAwait(false);

      var resultData = await resultTable_.CompleteResult(id.SessionId,
                                                         id.ResultId,
                                                         size,
                                                         opaqueId,
                                                         context.CancellationToken)
                                         .ConfigureAwait(false);

      await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                    resultTable_,
                                                    pushQueueStorage_,
                                                    sessionData,
                                                    new[]
                                                    {
                                                      id.ResultId,
                                                    },
                                                    logger_,
                                                    context.CancellationToken)
                               .ConfigureAwait(false);


      return new UploadResultDataResponse
             {
               Result = resultData.ToGrpcResultRaw(),
             };
    }
    catch (ResultNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while downloading results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Result data not found"));
    }
    catch (SessionNotFoundException e)
    {
      logger_.LogWarning(e,
                         "Error while downloading results");
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "Session associated to the result was not found"));
    }
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(GetResult))]
  public override async Task<GetResultResponse> GetResult(GetResultRequest  request,
                                                          ServerCallContext context)
  {
    using var measure = meter_.CountAndTime();
    var result = await resultTable_.GetResult(request.ResultId,
                                              context.CancellationToken)
                                   .ConfigureAwait(false);

    return new GetResultResponse
           {
             Result = result.ToGrpcResultRaw(),
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(ImportResultsData))]
  public override async Task<ImportResultsDataResponse> ImportResultsData(ImportResultsDataRequest request,
                                                                          ServerCallContext        context)
  {
    using var measure = meter_.CountAndTime();

    // early exit when there is nothing to do (mostly to please authentication test in which GetResults returns garbage)
    if (!request.Results.Any())
    {
      return new ImportResultsDataResponse();
    }

    var sessionData = await sessionTable_.GetSessionAsync(request.SessionId,
                                                          context.CancellationToken)
                                         .ConfigureAwait(false);

    var resultIds = request.Results.Select(id => id.ResultId)
                           .ToHashSet();

    var requests = request.Results.ToDictionary(id => id.ResultId,
                                                id => id.OpaqueId.ToByteArray());

    var dictTask = objectStorage_.GetSizesAsync(requests.Values,
                                                context.CancellationToken);

    var resultIdsDatabase = await resultTable_.GetResults(result => resultIds.Contains(result.ResultId),
                                                          result => new
                                                                    {
                                                                      result.ResultId,
                                                                      result.Status,
                                                                    },
                                                          context.CancellationToken)
                                              .ToDictionaryAsync(arg => arg.ResultId,
                                                                 arg => arg.Status)
                                              .ConfigureAwait(false);

    var dict = await dictTask.ConfigureAwait(false);

    Debug.Assert(dict.Count == request.Results.Count);

#if DEBUG
    foreach (var opaqueId in requests.Values)
    {
      Debug.Assert(dict.ContainsKey(opaqueId));
    }
#endif

    if (dict.Values.Any(size => size is null))
    {
      logger_.LogError("Opaque ids : {OpaqueIds} does not exist in the object storage",
                       dict.Where(pair => pair.Value is null)
                           .Select(pair => pair.Key)
                           .ToList());
      throw new RpcException(new Status(StatusCode.NotFound,
                                        "OpaqueId not found in the object storage"));
    }

    var intersection = resultIds.Except(resultIdsDatabase.Select(pair => pair.Key))
                                .ToList();

    if (intersection.Any())
    {
      logger_.LogError("Input result IDs {ResultIds} were not found in the database",
                       intersection);
      throw new RpcException(new Status(StatusCode.NotFound,
                                        $"Input result IDs were not found in the database: {intersection}"));
    }

    if (resultIdsDatabase.Any(pair => pair.Value != ResultStatus.Created))
    {
      var invalidResults = resultIdsDatabase.Where(pair => pair.Value != ResultStatus.Created)
                                            .ToList();
      logger_.LogError("Imported result should be in {Status} but {ResultIds} were not",
                       ResultStatus.Created,
                       invalidResults);
      throw new RpcException(new Status(StatusCode.FailedPrecondition,
                                        $"Imported results should be in {ResultStatus.Created} status, invalid results {invalidResults}"));
    }

    await resultTable_.CompleteManyResults(requests.Select(tuple => (tuple.Key, dict[tuple.Value]!.Value, tuple.Value)),
                                           context.CancellationToken)
                      .ConfigureAwait(false);

    await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                  resultTable_,
                                                  pushQueueStorage_,
                                                  sessionData,
                                                  resultIds,
                                                  logger_,
                                                  context.CancellationToken)
                             .ConfigureAwait(false);

    return new ImportResultsDataResponse
           {
             Results =
             {
               await resultTable_.GetResults(request.SessionId,
                                             resultIds,
                                             context.CancellationToken)
                                 .Select(result => result.ToGrpcResultRaw())
                                 .ToListAsync()
                                 .ConfigureAwait(false),
             },
           };
  }
}
