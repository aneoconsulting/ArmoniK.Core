// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using System.Collections.Generic;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <summary>
///   Options to fill partitions related data
/// </summary>
public class Partitioning
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(Partitioning);

  /// <summary>
  ///   Partitions in a JSON string to insert into the database
  /// </summary>
  public List<string> Partitions { get; init; } = new();
}
