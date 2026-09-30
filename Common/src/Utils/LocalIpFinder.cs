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
using System.Net.NetworkInformation;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Helper to get local IP address.
/// </summary>
[PublicAPI]
[Obsolete("Use ArmoniK.Core.Utils.LocalIpFinder")]
public class LocalIpFinder
{
  /// <summary>
  ///   Get local IPv4 address from a network interface.
  /// </summary>
  /// <param name="type">Interface type from which to get the IP.</param>
  /// <returns>
  ///   <see cref="string" /> representing the IP.
  /// </returns>
  public static string LocalIpv4Address(NetworkInterfaceType type = NetworkInterfaceType.Ethernet)
    => Core.Utils.LocalIpFinder.LocalIpv4Address(type);
}
