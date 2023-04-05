// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

namespace ArmoniK.Core.Common.Injection.Options;

/// <summary>
///   Configuration used to choose which adapter for storage is used
/// </summary>
[PublicAPI]
public class Components
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(Components);

  /// <summary>
  ///   Represents which database is used for tasks metadata
  /// </summary>
  public string? TableStorage { get; set; }

  /// <summary>
  ///   Represents which queue implementation is used to store messages
  /// </summary>
  public AdapterSettings QueueAdaptorSettings { get; set; } = new();

  /// <summary>
  ///   Represents which object storage is used to store data for tasks
  /// </summary>
  public string? ObjectStorage { get; set; }

  /// <summary>
  ///   Represents which database is used for authentication
  /// </summary>
  public string? AuthenticationStorage { get; set; }
}
