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
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Core.Common.Storage;

public interface IDispatchTable : IInitializable
{
  TimeSpan DispatchTimeToLiveDuration { get; }
  ILogger  Logger                     { get; }
  TimeSpan DispatchRefreshPeriod { get; }

  Task<bool> TryAcquireDispatchAsync(string                      sessionId,
                                     string                      taskId,
                                     string                      dispatchId,
                                     IDictionary<string, string> metadata,
                                     CancellationToken           cancellationToken = default);

  Task<Dispatch> GetDispatchAsync(string dispatchId, CancellationToken cancellationToken = default);

  Task AddStatusToDispatch(string id, TaskStatus status, CancellationToken cancellationToken = default);

  Task ExtendDispatchTtl(string id, CancellationToken cancellationToken = default);

  Task DeleteDispatchFromTaskIdAsync(string id, CancellationToken cancellationToken = default);

  Task DeleteDispatch(string id, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListDispatchAsync(string taskId, CancellationToken cancellationToken = default);

}
