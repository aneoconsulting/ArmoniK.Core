// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Threading;

using ArmoniK.Api.gRPC.V1;

using Grpc.Core;

using System.Threading.Tasks;

namespace ArmoniK.Core.Common.gRPC.Services
{
  public interface ISubmitter
  {
    Task<Empty> CancelSession(SessionId request, CancellationToken cancellationToken);
    Task<Empty> CancelTask(TaskFilter request, CancellationToken cancellationToken);
    Task<Count> CountTasks(TaskFilter request, CancellationToken cancellationToken);
    Task<CreateTaskReply> CreateLargeTasks(IAsyncEnumerable<CreateLargeTaskRequest> requestStream, CancellationToken cancellationToken);
    Task<CreateSessionReply> CreateSession(CreateSessionRequest request, CancellationToken cancellationToken);
    Task<CreateTaskReply> CreateSmallTasks(CreateSmallTaskRequest request, CancellationToken cancellationToken);
    Task<ConfigurationReply> GetServiceConfiguration(Empty request, CancellationToken cancellationToken);
    Task TryGetResult(ResultRequest request, IServerStreamWriter<ResultReply> responseStream, CancellationToken cancellationToken);
    Task<Count> WaitForCompletion(WaitRequest request, CancellationToken cancellationToken);
  }
}