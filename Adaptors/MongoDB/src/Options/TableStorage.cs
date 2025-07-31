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

namespace ArmoniK.Core.Adapters.MongoDB.Options;

/// <summary>
///   Represents the configuration settings for MongoDB table storage.
/// </summary>
[PublicAPI]
public class TableStorage
{
  /// <summary>
  ///   The configuration section path used to retrieve settings related to MongoDB table storage.
  /// </summary>
  public const string SettingSection = nameof(MongoDB) + ":" + nameof(TableStorage);

  /// <summary>
  ///   Minimum delay between polling attempts.
  ///   This defines the shortest interval the system will wait before
  ///   polling again, used as the initial delay.
  /// </summary>
  public TimeSpan PollingDelayMin { get; set; } = TimeSpan.FromSeconds(1);

  /// <summary>
  ///   Maximum delay between polling attempts.
  ///   This defines the upper bound for any exponential backoff
  ///   strategy applied during polling retries.
  /// </summary>
  public TimeSpan PollingDelayMax { get; set; } = TimeSpan.FromMinutes(5);
}
