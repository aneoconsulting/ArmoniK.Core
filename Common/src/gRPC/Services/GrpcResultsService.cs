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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Base;
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
                            ILogger<GrpcResultsService>                  logger)
  {
    logger_           = logger;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    sessionTable_     = sessionTable;
    objectStorage_    = objectStorage;
    pushQueueStorage_ = pushQueueStorage;
    meter_            = meter;
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
                                                          ResultStatus.Created,
                                                          new List<string>(),
                                                          DateTime.UtcNow,
                                                          0,
                                                          Array.Empty<byte>()))
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
    var results = await request.Results.Select(async rc =>
                                               {
                                                 var resultId = Guid.NewGuid()
                                                                    .ToString();

                                                 var size = await objectStorage_.AddOrUpdateAsync(resultId,
                                                                                                  new List<ReadOnlyMemory<byte>>
                                                                                                  {
                                                                                                    rc.Data.Memory,
                                                                                                  }.ToAsyncEnumerable(),
                                                                                                  context.CancellationToken)
                                                                                .ConfigureAwait(false);

                                                 return new Result(request.SessionId,
                                                                   resultId,
                                                                   rc.Name,
                                                                   request.SessionId,
                                                                   ResultStatus.Created,
                                                                   new List<string>(),
                                                                   DateTime.UtcNow,
                                                                   size,
                                                                   Array.Empty<byte>());
                                               })
                               .WhenAll()
                               .ConfigureAwait(false);

    await resultTable_.Create(results,
                              context.CancellationToken)
                      .ConfigureAwait(false);

    var resultList = await results.Select(async r => await resultTable_.CompleteResult(request.SessionId,
                                                                                       r.ResultId,
                                                                                       r.Size)
                                                                       .ConfigureAwait(false))
                                  .WhenAll()
                                  .ConfigureAwait(false);

    return new CreateResultsResponse
           {
             Results =
             {
               resultList.Select(r => r.ToGrpcResultRaw()),
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
      await objectStorage_.TryDeleteAsync(request.ResultId,
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
                         "Error while downloading results");
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
      await foreach (var chunk in objectStorage_.GetValuesAsync(request.ResultId,
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

    long size = 0;

    await objectStorage_.AddOrUpdateAsync(id.ResultId,
                                          requestStream.ReadAllAsync(context.CancellationToken)
                                                       .Select(r =>
                                                               {
                                                                 size += r.DataChunk.Length;
                                                                 return r.DataChunk.Memory;
                                                               }),
                                          context.CancellationToken)
                        .ConfigureAwait(false);

    try
    {
      var sessionData = await sessionTask.ConfigureAwait(false);

      var resultData = await resultTable_.CompleteResult(id.SessionId,
                                                         id.ResultId,
                                                         size,
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
}
