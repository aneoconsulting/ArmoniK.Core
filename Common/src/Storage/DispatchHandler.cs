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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

namespace ArmoniK.Core.Common.Storage;

public class DispatchHandler : IDispatch, IAsyncDisposable
{
  private readonly IDispatch        dispatchImplementation_;
  private          IAsyncDisposable asyncDisposableImplementation_;

  public DispatchHandler(IDispatch         dispatchImplementation,
                         ITableStorage     tableStorage,
                         CancellationToken cancellationToken)
  {
    dispatchImplementation_ = dispatchImplementation;
    Heart dispatchRefresher = new(async token =>
                                  {
                                    await tableStorage.ExtendDispatchTtl(Id,
                                                                         DateTime.UtcNow + tableStorage.DispatchTimeToLiveDuration,
                                                                         token);
                                    return !token.IsCancellationRequested;
                                  },
                                  tableStorage.DispatchRefreshPeriod,
                                  cancellationToken);
    dispatchRefresher.Start();

    asyncDisposableImplementation_ = AsyncDisposable.Create(async () => { await dispatchRefresher.Stop(); });
  }

  /// <inheritdoc />
  public string Id => dispatchImplementation_.Id;

  /// <inheritdoc />
  public string TaskId => dispatchImplementation_.TaskId;

  /// <inheritdoc />
  public int Attempt => dispatchImplementation_.Attempt;

  /// <inheritdoc />
  public DateTime TimeToLive => dispatchImplementation_.TimeToLive;

  /// <inheritdoc />
  public IEnumerable<IDispatch.StatusTime> Statuses => dispatchImplementation_.Statuses;

  /// <inheritdoc />
  public DateTime CreationDate => dispatchImplementation_.CreationDate;

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    if (asyncDisposableImplementation_ is not null)
    {
      await asyncDisposableImplementation_.DisposeAsync();
      asyncDisposableImplementation_ = null;
    }

    GC.SuppressFinalize(this);
  }
}
