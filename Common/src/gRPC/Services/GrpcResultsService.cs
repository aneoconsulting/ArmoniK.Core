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
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.gRPC.Services;

[Authorize(AuthenticationSchemes = Authenticator.SchemeName)]
public class GrpcResultsService : Results.ResultsBase
{
  private readonly ILogger<GrpcResultsService> logger_;
  private readonly IObjectStorage              objectStorage_;
  private readonly IPushQueueStorage           pushQueueStorage_;
  private readonly IResultTable                resultTable_;
  private readonly ITaskTable                  taskTable_;

  public GrpcResultsService(IResultTable                resultTable,
                            ITaskTable                  taskTable,
                            IObjectStorage              objectStorage,
                            IPushQueueStorage           pushQueueStorage,
                            ILogger<GrpcResultsService> logger)
  {
    logger_           = logger;
    resultTable_      = resultTable;
    taskTable_        = taskTable;
    objectStorage_    = objectStorage;
    pushQueueStorage_ = pushQueueStorage;
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(GetOwnerTaskId))]
  public override async Task<GetOwnerTaskIdResponse> GetOwnerTaskId(GetOwnerTaskIdRequest request,
                                                                    ServerCallContext     context)
  {
    using var _ = logger_.LogFunction();

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
               results.results.Select(result => new ResultRaw(result)),
             },
             Total = results.totalCount,
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(CreateResultsMetaData))]
  public override async Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                                  ServerCallContext            context)
  {
    var results = request.Results.Select(rc => new Result(request.SessionId,
                                                          Guid.NewGuid()
                                                              .ToString(),
                                                          rc.Name,
                                                          "",
                                                          ResultStatus.Created,
                                                          new List<string>(),
                                                          DateTime.UtcNow,
                                                          Array.Empty<byte>()))
                         .ToList();

    await resultTable_.Create(results,
                              context.CancellationToken)
                      .ConfigureAwait(false);

    return new CreateResultsMetaDataResponse
           {
             Results =
             {
               results.Select(result => new ResultRaw(result)),
             },
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(CreateResults))]
  public override async Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                                  ServerCallContext    context)
  {
    var results = await request.Results.Select(async rc =>
                                               {
                                                 var result = new Result(request.SessionId,
                                                                         Guid.NewGuid()
                                                                             .ToString(),
                                                                         rc.Name,
                                                                         "",
                                                                         ResultStatus.Created,
                                                                         new List<string>(),
                                                                         DateTime.UtcNow,
                                                                         Array.Empty<byte>());

                                                 await objectStorage_.AddOrUpdateAsync(result.ResultId,
                                                                                       new List<ReadOnlyMemory<byte>>
                                                                                       {
                                                                                         rc.Data.Memory,
                                                                                       }.ToAsyncEnumerable(),
                                                                                       context.CancellationToken)
                                                                     .ConfigureAwait(false);

                                                 return result;
                                               })
                               .WhenAll()
                               .ConfigureAwait(false);

    await resultTable_.Create(results,
                              context.CancellationToken)
                      .ConfigureAwait(false);

    var resultList = await results.Select(async r => await resultTable_.CompleteResult(request.SessionId,
                                                                                       r.ResultId)
                                                                       .ConfigureAwait(false))
                                  .WhenAll()
                                  .ConfigureAwait(false);

    await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                  resultTable_,
                                                  pushQueueStorage_,
                                                  request.SessionId,
                                                  resultList.Select(result => result.ResultId)
                                                            .AsICollection(),
                                                  logger_,
                                                  context.CancellationToken)
                             .ConfigureAwait(false);

    return new CreateResultsResponse
           {
             Results =
             {
               resultList.Select(r => new ResultRaw(r)),
             },
           };
  }

  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(DeleteResultsData))]
  public override async Task<DeleteResultsDataResponse> DeleteResultsData(DeleteResultsDataRequest request,
                                                                          ServerCallContext        context)
  {
    try
    {
      await request.ResultId.Select(resultId => objectStorage_.TryDeleteAsync(resultId,
                                                                              context.CancellationToken))
                   .WhenAll()
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
    => Task.FromResult(new ResultsServiceConfigurationResponse
                       {
                         DataChunkMaxSize = PayloadConfiguration.MaxChunkSize,
                       });


  [RequiresPermission(typeof(GrpcResultsService),
                      nameof(UploadResultData))]
  public override async Task<UploadResultDataResponse> UploadResultData(IAsyncStreamReader<UploadResultDataRequest> requestStream,
                                                                        ServerCallContext                           context)
  {
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


    await objectStorage_.AddOrUpdateAsync(id.ResultId,
                                          requestStream.ReadAllAsync(context.CancellationToken)
                                                       .Select(r => r.DataChunk.Memory),
                                          context.CancellationToken)
                        .ConfigureAwait(false);

    try
    {
      var resultData = await resultTable_.CompleteResult(id.SessionId,
                                                         id.ResultId,
                                                         context.CancellationToken)
                                         .ConfigureAwait(false);

      await TaskLifeCycleHelper.ResolveDependencies(taskTable_,
                                                    resultTable_,
                                                    pushQueueStorage_,
                                                    id.SessionId,
                                                    new[]
                                                    {
                                                      id.ResultId,
                                                    },
                                                    logger_,
                                                    context.CancellationToken)
                               .ConfigureAwait(false);


      return new UploadResultDataResponse
             {
               Result = resultData,
             };
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
                      nameof(GetResult))]
  public override async Task<GetResultResponse> GetResult(GetResultRequest  request,
                                                          ServerCallContext context)
  {
    var result = await resultTable_.GetResult("",
                                              request.ResultId,
                                              context.CancellationToken)
                                   .ConfigureAwait(false);

    return new GetResultResponse
           {
             Result = result,
           };
  }
}
