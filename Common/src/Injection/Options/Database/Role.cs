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
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <summary>
///   Associate a Role to the permission it has
/// </summary>
public record Role
{
  /// <summary>
  ///   Role Name
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  ///   Permissions associated to the Role
  /// </summary>
  public required List<string> Permissions { get; init; }

  /// <inheritdoc />
  public virtual bool Equals(Role? other)
    => !ReferenceEquals(null,
                        other) && Name.Equals(other.Name) && Permissions.SequenceEqual(other.Permissions);

  /// <summary>
  ///   Convert <inheritdoc cref="Role" /> to JSON
  /// </summary>
  /// <returns>
  ///   <inheritdoc cref="string" /> representing the JSON object of this instance
  /// </returns>
  public string ToJson()
    => JsonSerializer.Serialize(this);

  /// <summary>
  ///   Build a <inheritdoc cref="Role" /> from a JSON <inheritdoc cref="string" />
  /// </summary>
  /// <param name="json">JSON value</param>
  /// <returns>
  ///   <inheritdoc cref="Role" /> built from the provided JSON
  /// </returns>
  public static Role FromJson(string json)
    => JsonSerializer.Deserialize<Role>(json)!;

  /// <inheritdoc />
  public override int GetHashCode()
    => HashCode.Combine(Name,
                        Permissions);
}
