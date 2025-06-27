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

using System;

namespace ArmoniK.Core.Adapters.Redis.Options;

/// <summary>
///   Represents the configuration settings for connecting to a Redis instance.
/// </summary>
public class Redis
{
  /// <summary>
  ///   The name of the configuration section for Redis settings.
  /// </summary>
  public const string SettingSection = nameof(Redis);

  /// <summary>
  ///   Name of the Redis instance.
  /// </summary>
  public string InstanceName { get; set; } = "";

  /// <summary>
  ///   Endpoint URL for the Redis server.
  /// </summary>
  public string EndpointUrl { get; set; } = "";

  /// <summary>
  ///   Client connecting to the Redis server.
  /// </summary>
  public string ClientName { get; set; } = "";

  /// <summary>
  ///   Hostname for SSL connections.
  /// </summary>
  public string SslHost { get; set; } = "";

  /// <summary>
  ///   Timeout duration (in milliseconds) for Redis operations.
  /// </summary>
  public int Timeout { get; set; }

  /// <summary>
  ///   Password for authenticating with the Redis server.
  /// </summary>
  public string Password { get; set; } = "";

  /// <summary>
  ///   Username for authenticating with the Redis server.
  /// </summary>
  public string User { get; set; } = "";

  /// <summary>
  ///   Whether to use SSL for the connection.
  /// </summary>
  public bool Ssl { get; set; }

  /// <summary>
  ///   Path to the credentials file for authentication.
  /// </summary>
  public string CredentialsPath { get; set; } = "";

  /// <summary>
  ///   Path to the Certificate Authority (CA) file for SSL connections.
  /// </summary>
  public string CaPath { get; set; } = "";

  /// <summary>
  ///   Maximum number of retry attempts for failed operations.
  /// </summary>
  public int MaxRetry { get; set; } = 5;

  /// <summary>
  ///   Duration (in milliseconds) to wait after a retry attempt.
  /// </summary>
  public int MsAfterRetry { get; set; } = 500;

  /// <summary>
  ///   Key expiration time (TTL) for keys in the Redis database.
  /// </summary>
  public TimeSpan TtlTimeSpan { get; set; } = TimeSpan.MaxValue;

  /// <summary>
  ///   Whether to allow host name mismatches in SSL certificates.
  /// </summary>
  public bool AllowHostMismatch { get; set; }
}
