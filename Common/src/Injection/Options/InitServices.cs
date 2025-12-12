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

using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Utils.DocAttribute;

namespace ArmoniK.Core.Common.Injection.Options;

/// <summary>
///   Configuration for ArmoniK services
/// </summary>
[ExtractDocumentation("Options for InitServices")]
public class InitServices
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(InitServices);

  /// <summary>
  ///   Authentication configurations
  /// </summary>
  public Authentication Authentication { get; set; } = new();

  /// <summary>
  ///   Partitioning configurations
  /// </summary>
  public Partitioning Partitioning { get; set; } = new();

  /// <summary>
  ///   Whether to perform database initialization (collection creation, indexing, sharding, data insertion, etc...).
  /// </summary>
  public bool InitDatabase { get; set; } = true;


  /// <summary>
  ///   Whether to perform object storage initialization
  /// </summary>
  public bool InitObjectStorage { get; set; } = true;

  /// <summary>
  ///   Whether to perform queue initialization
  /// </summary>
  public bool InitQueue { get; set; } = true;

  /// <summary>
  ///   Stop the service after performing initialization
  /// </summary>
  public bool StopAfterInit { get; set; } = false;
}
