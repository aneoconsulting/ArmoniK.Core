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

using System;
using System.Threading;
using System.Threading.Tasks;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public interface ILockedQueueStorage : IQueueStorageBase
  {
    TimeSpan LockRefreshPeriodicity { get; }

    TimeSpan LockRefreshExtension { get; }

    bool AreMessagesUnique { get; }

    Task<bool> RenewDeadlineAsync(string id, CancellationToken cancellationToken = default);


    /// <summary>
    ///   Indicates that the message was successfully processed
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task MessageProcessedAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    ///   Indicates that the message is poisonous
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task MessageRejectedAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    ///   Places the message in the back of the queue
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task RequeueMessageAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    ///   Places the message in the front of the queue
    /// </summary>
    /// <param name="id"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task ReleaseMessageAsync(string id, CancellationToken cancellationToken = default);
  }
}
