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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

namespace ArmoniK.Core.Adapters.Couchbase.Options
{
  /// <summary>
  ///   Represents the configuration settings for connecting to a Couchbase cluster.
  /// </summary>
  public class CouchbaseSettings
  {
    /// <summary>
    ///   The name of the configuration section for Couchbase settings.
    /// </summary>
    public const string SettingSection = "Couchbase";
    
    /// <summary>
    ///   Username for authenticating with the Couchbase server.
    /// </summary>
    public string Login { get; init; } = string.Empty;
    
    /// <summary>
    ///   Password for authenticating with the Couchbase server.
    /// </summary>
    public string Password { get; init; } = string.Empty;
    
    /// <summary>
    ///   Connection string for the Couchbase cluster (e.g., "couchbase://localhost").
    /// </summary>
    public string ConnectionString { get; init; } = string.Empty;
    
    /// <summary>
    ///   Whether to use TLS/SSL for secure connections to the Couchbase cluster.
    /// </summary>
    public bool IsTls { get; init; }
    
    /// <summary>
    ///   Timeout duration for Key-Value operations. Default is 10 seconds.
    /// </summary>
    public TimeSpan KvTimeout { get; init; } = TimeSpan.FromSeconds(10);
    
    /// <summary>
    ///   Timeout duration for N1QL query operations. Default is 75 seconds.
    /// </summary>
    public TimeSpan QueryTimeout { get; init; } = TimeSpan.FromSeconds(75);
    
    /// <summary>
    ///   Timeout duration for management API operations (e.g., bucket/user management). Default is 75 seconds.
    /// </summary>
    public TimeSpan ManagementTimeout { get; init; } = TimeSpan.FromSeconds(75);
    
    /// <summary>
    ///   Initial number of Key-Value connections to establish per node. Default is 1.
    /// </summary>
    public int NumKvConnections { get; init; } = 1;
    
    /// <summary>
    ///   Whether to enable TCP keep-alive for connections. Default is true.
    /// </summary>
    public bool EnableTcpKeepAlive { get; init; } = true;
    
    /// <summary>
    ///   Time duration before sending the first TCP keep-alive probe. Default is 60 seconds.
    /// </summary>
    public TimeSpan TcpKeepAliveTime { get; init; } = TimeSpan.FromSeconds(60);
    
    /// <summary>
    ///   Interval between TCP keep-alive probes. Default is 1 second.
    /// </summary>
    public TimeSpan TcpKeepAliveInterval { get; init; } = TimeSpan.FromSeconds(1);
    
    /// <summary>
    ///   Maximum number of Key-Value connections allowed per node. Default is 5.
    /// </summary>
    public int MaxKvConnections { get; init; } = 5;
    
    /// <summary>
    ///   Whether to enable operation duration tracing for performance monitoring. Default is true.
    /// </summary>
    public bool EnableOperationDurationTracing { get; init; } = true;
  }
}
