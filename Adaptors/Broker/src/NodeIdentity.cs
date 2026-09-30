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
using System.Net;

using ArmoniK.Core.Utils;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Resolves the identifier of the node declared to the broker for task and data affinity.
/// </summary>
internal static class NodeIdentity
{
  /// <summary>
  ///   <see cref="Broker.NodeId" /> when set, the value given by <see cref="Broker.NodeIdSource" /> otherwise.
  /// </summary>
  /// <exception cref="InvalidOperationException">The source gives no value</exception>
  public static string Resolve(Broker options)
  {
    if (!string.IsNullOrEmpty(options.NodeId))
    {
      return options.NodeId;
    }

    var id = options.NodeIdSource switch
             {
               Broker.IdSource.HostName => Dns.GetHostName(),
               Broker.IdSource.Ip       => LocalIpFinder.LocalIpv4Address(),
               Broker.IdSource.NodeName => Environment.GetEnvironmentVariable("NODE_NAME"),
               _                     => throw new ArgumentOutOfRangeException(nameof(options)),
             };
    return string.IsNullOrEmpty(id)
             ? throw new InvalidOperationException($"{Broker.SettingSection}:{nameof(Broker.NodeIdSource)} is {options.NodeIdSource} but gives no identifier; inject NODE_NAME from spec.nodeName or set {Broker.SettingSection}:{nameof(Broker.NodeId)}")
             : id;
  }
}
