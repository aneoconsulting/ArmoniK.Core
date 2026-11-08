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

using ArmoniK.Api.Common.Options;

namespace ArmoniK.Core.Standalone;

/// <summary>
///   Options of the standalone host, read from the <c>Standalone</c> section of the configuration
/// </summary>
public class StandaloneOptions
{
  /// <summary>
  ///   Name of the configuration section
  /// </summary>
  public const string SettingSection = "Standalone";

  /// <summary>
  ///   Number of compute slots. Each slot runs one polling agent, connected to one worker.
  /// </summary>
  public int Slots { get; set; } = 1;

  /// <summary>
  ///   Folder holding the object storage, the caches of the slots and their sockets. Relative paths are resolved from the
  ///   current directory.
  /// </summary>
  public string DataFolder { get; set; } = "data";

  /// <summary>
  ///   Port of the gRPC API (HTTP/2 without TLS)
  /// </summary>
  public int GrpcPort { get; set; } = 5001;

  /// <summary>
  ///   Port of gRPC-Web and of the health checks (HTTP/1.1)
  /// </summary>
  public int HttpPort { get; set; } = 5002;

  /// <summary>
  ///   Whether to listen on all the network interfaces. By default, only the local machine can connect, since
  ///   authentication is disabled.
  /// </summary>
  public bool ListenAnyIP { get; set; }

  /// <summary>
  ///   Type of the sockets between the slots and their workers
  /// </summary>
  public GrpcSocketType SocketType { get; set; } = GrpcSocketType.UnixDomainSocket;

  /// <summary>
  ///   Address where the worker of a slot listens: a socket path, or a URI for TCP.
  ///   <c>{data}</c> is replaced by the data folder and <c>{slot}</c> by the index of the slot.
  /// </summary>
  public string WorkerAddress { get; set; } = "{data}/slot{slot}/worker.sock";

  /// <summary>
  ///   Address where the agent of a slot listens for its worker: a socket path, or a URI for TCP.
  ///   <c>{data}</c> is replaced by the data folder and <c>{slot}</c> by the index of the slot.
  /// </summary>
  public string AgentAddress { get; set; } = "{data}/slot{slot}/agent.sock";
}
