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

using JetBrains.Annotations;

namespace ArmoniK.Core.Adapters.PostgresSQL.Options;

/// <summary>
///   Represents the configuration settings for PostgreSQL connection.
/// </summary>
[PublicAPI]
public class PostgreSQL
{
  /// <summary>
  ///   The configuration section path used to retrieve PostgreSQL settings.
  /// </summary>
  public const string SettingSection = nameof(PostgreSQL);

  /// <summary>
  ///   PostgreSQL host
  /// </summary>
  public string Host { get; set; } = "localhost";

  /// <summary>
  ///   PostgreSQL port
  /// </summary>
  public int Port { get; set; } = 5432;

  /// <summary>
  ///   PostgreSQL username
  /// </summary>
  public string? User { get; set; }

  /// <summary>
  ///   PostgreSQL password
  /// </summary>
  public string? Password { get; set; }

  /// <summary>
  ///   PostgreSQL database name
  /// </summary>
  public string DatabaseName { get; set; } = "armonik";

  /// <summary>
  ///   Full connection string (overrides Host/Port/User/Password/DatabaseName if set)
  /// </summary>
  public string? ConnectionString { get; set; }

  /// <summary>
  ///   Whether to use SSL
  /// </summary>
  public bool Ssl { get; set; }

  /// <summary>
  ///   Path to the credentials file
  /// </summary>
  public string? CredentialsPath { get; set; }

  /// <summary>
  ///   Maximum connection pool size
  /// </summary>
  public int MaxPoolSize { get; set; } = 100;
}
