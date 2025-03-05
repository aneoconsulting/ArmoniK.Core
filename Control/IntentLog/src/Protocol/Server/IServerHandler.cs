// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using JetBrains.Annotations;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Server;

[PublicAPI]
public interface IServerHandler
{
  public Task OpenAsync(Intent            intent,
                        byte[]            payload,
                        CancellationToken cancellationToken = default);

  public Task AmendAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);

  public Task CloseAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);

  public Task AbortAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);

  public Task TimeoutAsync(Intent            intent,
                           byte[]            payload,
                           CancellationToken cancellationToken = default);

  public Task ResetAsync(Intent            intent,
                         byte[]            payload,
                         CancellationToken cancellationToken = default);
}
