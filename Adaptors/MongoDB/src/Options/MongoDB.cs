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

using ArmoniK.Utils.DocAttribute;

using JetBrains.Annotations;

// ReSharper disable InconsistentNaming

namespace ArmoniK.Core.Adapters.MongoDB.Options;

/// <summary>
///   Represents the configuration settings for connecting to a MongoDB database.
/// </summary>
[PublicAPI]
[ExtractDocumentation("Options for MongoDB")]
public class MongoDB
{
  /// <summary>
  ///   The name of the configuration section for MongoDB settings.
  /// </summary>
  public const string SettingSection = nameof(MongoDB);

  /// <summary>
  ///   Whether to use TLS for the connection.
  /// </summary>
  public bool Tls { get; set; }

  /// <summary>
  ///   Whether to allow insecure TLS connections.
  /// </summary>
  public bool AllowInsecureTls { get; set; }

  /// <summary>
  ///   Whether to connect directly to the MongoDB server, bypassing the replica set.
  /// </summary>
  public bool DirectConnection { get; set; }

  /// <summary>
  ///   Name of the replica set to connect to.
  /// </summary>
  public string ReplicaSet { get; set; } = "";

  /// <summary>
  ///   Hostname of the MongoDB server.
  /// </summary>
  public string Host { get; set; } = "";

  /// <summary>
  ///   Port number for the MongoDB server connection.
  /// </summary>
  public int Port { get; set; }

  /// <summary>
  ///   Path to the Certificate Authority (CA) file for TLS connections.
  /// </summary>
  public string CAFile { get; set; } = "";

  /// <summary>
  ///   Path to the credentials file for authentication.
  /// </summary>
  public string CredentialsPath { get; set; } = "";

  /// <summary>
  ///   Username for connecting to the MongoDB server.
  /// </summary>
  public string User { get; set; } = "";

  /// <summary>
  ///   Connection string for the MongoDB server.
  ///   If not null or empty, the MongoClientSettings are derived from it.
  ///   Other connection options like Host, Port, User, Password and DataBaseName are ignored in this case. That is,
  ///   the connection options provided in the ConnectionString take precedence over the  connection options defined
  ///   in this class.
  /// </summary>
  public string ConnectionString { get; set; } = "";

  /// <summary>
  ///   Password for connecting to the MongoDB server.
  /// </summary>
  public string Password { get; set; } = "";

  /// <summary>
  ///   Maximum number of retry attempts for failed operations.
  /// </summary>
  public int MaxRetries { get; set; } = 5;

  /// <summary>
  ///   Name of the database to connect to.
  /// </summary>
  public string DatabaseName { get; set; } = "ArmoniK";

  /// <summary>
  ///   Duration for which data should be retained in the database.
  /// </summary>
  public TimeSpan DataRetention { get; set; } = TimeSpan.MaxValue;

  /// <summary>
  ///   Table storage configuration.
  /// </summary>
  public TableStorage TableStorage { get; set; } = new();

  /// <summary>
  ///   Maximum size of the connection pool.
  /// </summary>
  public int MaxConnectionPoolSize { get; set; } = 500;

  /// <summary>
  ///   Timeout duration for server selection.
  /// </summary>
  public TimeSpan ServerSelectionTimeout { get; set; } = TimeSpan.FromMinutes(2);

  /// <summary>
  ///   Whether sharding is enabled for the database.
  /// </summary>
  public bool Sharding { get; set; }

  /// <summary>
  ///   Authentication source for the MongoDB connection.
  /// </summary>
  public string AuthSource { get; set; } = "";

  /// <summary>
  ///   Indicates whether to use minimal indexes for the collections.
  /// </summary>
  public bool UseMinimalIndexes { get; set; } = false;

}
