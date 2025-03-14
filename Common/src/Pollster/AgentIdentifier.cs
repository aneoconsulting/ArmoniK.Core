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

using System.Net;

using ArmoniK.Core.Common.Utils;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Represents an identifier for an agent, including the owner's pod ID and pod name.
/// </summary>
public class AgentIdentifier
{
  /// <summary>
  ///   Gets the owner's pod ID, which is the local IPv4 address.
  /// </summary>
  public readonly string OwnerPodId = LocalIpFinder.LocalIpv4Address();

  /// <summary>
  ///   Gets the owner's pod name, which is the host name.
  /// </summary>
  public readonly string OwnerPodName = Dns.GetHostName();
}
