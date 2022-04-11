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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

namespace ArmoniK.Core.Common.Storage;

public record DispatchHandler : Dispatch, IAsyncDisposable
{
  private IAsyncDisposable? asyncDisposableImplementation_;

  public DispatchHandler(IDispatchTable    dispatchTable,
                         ITaskTable        taskTable,
                         Dispatch          dispatchImplementation,
                         CancellationToken cancellationToken)
    : base(dispatchImplementation)
  {
    var dispatchTable1 = dispatchTable;
    Heart dispatchRefresher = new(async token =>
                                  {
                                    var ttlTask = dispatchTable1.ExtendDispatchTtl(Id,
                                                                                   token);

                                    var status = await taskTable.IsTaskCancelledAsync(TaskId,
                                                                                      token)
                                                                .ConfigureAwait(false);
                                    await ttlTask.ConfigureAwait(false);

                                    return !status && !token.IsCancellationRequested;
                                  },
                                  dispatchTable1.DispatchRefreshPeriod,
                                  cancellationToken);
    dispatchRefresher.Start();
    DispatchCancelled = dispatchRefresher.HeartStopped;

    asyncDisposableImplementation_ = AsyncDisposable.Create(async () =>
                                                            {
                                                              await dispatchRefresher.Stop()
                                                                                     .ConfigureAwait(false);
                                                            });
  }

  public CancellationToken DispatchCancelled { get; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    if (asyncDisposableImplementation_ is not null)
    {
      await asyncDisposableImplementation_.DisposeAsync()
                                          .ConfigureAwait(false);
      asyncDisposableImplementation_ = null;
    }

    GC.SuppressFinalize(this);
  }
}
