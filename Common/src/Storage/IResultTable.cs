// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Storage;

public interface IResultTable : IInitializable
{
  Task<bool> AreResultsAvailableAsync(string sessionId, IEnumerable<string> keys, CancellationToken cancellationToken = default);

  Task ChangeResultDispatch(string sessionId, string oldDispatchId, string newDispatchId, CancellationToken cancellationToken);

  Task ChangeResultOwnership(string sessionId, IEnumerable<string> keys, string oldTaskId, string newTaskId, CancellationToken cancellationToken);

  Task Create(IEnumerable<Result> results, CancellationToken cancellationToken = default);

  Task DeleteResult(string session, string key, CancellationToken cancellationToken = default);

  Task DeleteResults(string sessionId, CancellationToken cancellationToken = default);

  Task<Result> GetResult(string sessionId, string key, CancellationToken cancellationToken = default);

  IAsyncEnumerable<string> ListResultsAsync(string sessionId, CancellationToken cancellationToken = default);

  Task SetResult(string sessionId, string ownerTaskId, string key, byte[] smallPayload, CancellationToken cancellationToken = default);

  Task SetResult(string sessionId, string ownerTaskId, string key, CancellationToken cancellationToken = default);
}
