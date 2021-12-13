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

using ArmoniK.Core.gRPC.V1;

namespace ArmoniK.Core.Storage
{
  public interface ILeaseProvider
  {
    TimeSpan AcquisitionPeriod { get; }

    TimeSpan AcquisitionDuration { get; }

    /// <summary>
    ///   Try to acquire a lease to process the task. If processing will last after the expiration date, the Lease will have to
    ///   be renewed.
    /// </summary>
    /// <param name="id">The Id of the task to process.</param>
    /// <param name="cancellationToken">Cancellation token for the request</param>
    /// <returns>The lease to be used for renewal or an empty leaseId and past expiration in case of failure</returns>
    Task<Lease> TryAcquireLeaseAsync(TaskId id, CancellationToken cancellationToken = default);

    Task<Lease> TryRenewLease(TaskId id, string leaseId, CancellationToken cancellationToken = default);

    Task ReleaseLease(TaskId id, string leaseId, CancellationToken cancellationToken = default);
  }
}
