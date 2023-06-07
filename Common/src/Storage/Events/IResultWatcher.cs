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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;

namespace ArmoniK.Core.Common.Storage.Events;

/// <summary>
///   Interface used to receive events when Results are modified
/// </summary>
public interface IResultWatcher : IInitializable
{
  /// <summary>
  ///   Receive a <see cref="NewResult" /> event when a new result is added in the given session
  /// </summary>
  /// <param name="sessionId">The session id</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A <see cref="IAsyncEnumerable{NewResult}" /> that holds the updates when they are available
  /// </returns>
  Task<IAsyncEnumerable<NewResult>> GetNewResults(string            sessionId,
                                                  CancellationToken cancellationToken = default);

  /// <summary>
  ///   Receive a <see cref="ResultOwnerUpdate" /> event when the OwnerId of a result changes in the given session
  /// </summary>
  /// <param name="sessionId">The session id</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A <see cref="IAsyncEnumerable{ResultOwnerUpdate}" /> that holds the updates when they are available
  /// </returns>
  Task<IAsyncEnumerable<ResultOwnerUpdate>> GetResultOwnerUpdates(string            sessionId,
                                                                  CancellationToken cancellationToken = default);

  /// <summary>
  ///   Receive a <see cref="ResultStatusUpdate" /> event when the Status of a result changes in the given session
  /// </summary>
  /// <param name="sessionId">The session id</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>
  ///   A <see cref="IAsyncEnumerable{ResultStatusUpdate}" /> that holds the updates when they are available
  /// </returns>
  Task<IAsyncEnumerable<ResultStatusUpdate>> GetResultStatusUpdates(string            sessionId,
                                                                    CancellationToken cancellationToken = default);
}
