// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using System;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Values fixed by the protocol (<c>Broker/docs/protocol.md</c>), not by configuration.
/// </summary>
internal static class Protocol
{
  /// <summary>
  ///   Highest priority: priorities range from 1 to 16 and are stored on 4 bits by the server (§3).
  /// </summary>
  public const int MaxPriority = 16;

  /// <summary>
  ///   First delay of the exponential back-off on retryable failures (§5).
  /// </summary>
  public static readonly TimeSpan BackoffMin = TimeSpan.FromMilliseconds(100);

  /// <summary>
  ///   Largest delay of the exponential back-off on retryable failures (§5).
  /// </summary>
  public static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(10);
}
