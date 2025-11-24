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

using JetBrains.Annotations;

// ReSharper disable InconsistentNaming

namespace ArmoniK.Core.Adapters.MongoDB.Options;

[PublicAPI]
public class MongoDB
{
  public const string SettingSection = nameof(MongoDB);

  public bool Tls { get; set; }

  public bool AllowInsecureTls { get; set; }

  public bool DirectConnection { get; set; }

  public string ReplicaSet { get; set; } = "";

  public string Host { get; set; } = "";

  public int Port { get; set; }

  public string CAFile { get; set; } = "";

  public string CredentialsPath { get; set; } = "";

  public string User { get; set; } = "";

  public string ConnectionString { get; set; } = "";

  public string Password { get; set; } = "";

  public int MaxRetries { get; set; } = 5;

  public string DatabaseName { get; set; } = "ArmoniK";

  public TimeSpan DataRetention { get; set; } = TimeSpan.MaxValue;

  public TableStorage TableStorage { get; set; } = new();

  public int MaxConnectionPoolSize { get; set; } = 500;

  public TimeSpan ServerSelectionTimeout { get; set; } = TimeSpan.FromMinutes(2);

  public bool Sharding { get; set; }

  public string AuthSource { get; set; } = "";

  /// <summary>
  ///   Indicates whether to use minimal indexes for the collections.
  /// </summary>
  public bool UseMinimalIndexes { get; set; } = false;
}
