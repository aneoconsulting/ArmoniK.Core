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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Agent;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class SimpleAgent : IAgent
{
  public string Token
    => "token";

  public string Folder
    => "folder";

  public string SessionId
    => "session";

  public Task FinalizeTaskCreation(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public Task<string> GetCommonData(string            token,
                                    string            resultId,
                                    CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public Task<string> GetDirectData(string            token,
                                    string            resultId,
                                    CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public Task<string> GetResourceData(string            token,
                                      string            resultId,
                                      CancellationToken cancellationToken)
    => throw new NotImplementedException();

  public Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                   CancellationToken            cancellationToken)
    => Task.FromResult(new CreateResultsMetaDataResponse());

  public Task<ICollection<TaskCreationRequest>> SubmitTasks(ICollection<TaskSubmissionRequest> requests,
                                                            TaskOptions?                       taskOptions,
                                                            string                             sessionId,
                                                            string                             token,
                                                            CancellationToken                  cancellationToken)
    => Task.FromResult(new List<TaskCreationRequest>().AsICollection());

  public Task<CreateResultsResponse> CreateResults(CreateResultsRequest request,
                                                   CancellationToken    cancellationToken)
    => Task.FromResult(new CreateResultsResponse());

  public Task<ICollection<string>> NotifyResultData(string              token,
                                                    ICollection<string> resultIds,
                                                    CancellationToken   cancellationToken)
    => Task.FromResult(Array.Empty<string>()
                            .AsICollection());

  public Task CancelChildTasks(CancellationToken cancellationToken)
    => Task.CompletedTask;

  public void Dispose()
    => GC.SuppressFinalize(this);
}
