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

using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.QueueCommon;

namespace ArmoniK.Core.Adapters.Amqp;

public class QueueStorage : QueueStorageBase
{
  private const      int             MaxInternalQueuePriority = 10;
  protected readonly IConnectionAmqp ConnectionAmqp;

  protected readonly int NbLinks;

  public QueueStorage(QueueCommon.Amqp options,
                      IConnectionAmqp  connectionAmqp)
    : base(options)
  {
    ConnectionAmqp = connectionAmqp;
    NbLinks        = (MaxPriority + MaxInternalQueuePriority - 1) / MaxInternalQueuePriority;
  }

  /// <inheritdoc />
  public override async Task Init(CancellationToken cancellationToken)
  {
    await ConnectionAmqp.Init(cancellationToken)
                        .ConfigureAwait(false);

    if (!IsInitialized)
    {
      IsInitialized = true;
    }
  }
}
