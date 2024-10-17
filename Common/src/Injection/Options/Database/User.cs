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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <summary>
///   Associate a User to its Roles
/// </summary>
public record User
{
  /// <summary>
  ///   User Name
  /// </summary>
  public required string Name { get; init; }

  /// <summary>
  ///   Roles associated to the user
  /// </summary>
  public required List<string> Roles { get; init; }

  /// <inheritdoc />
  public virtual bool Equals(User? other)
    => !ReferenceEquals(null,
                        other) && Name.Equals(other.Name) && Roles.SequenceEqual(other.Roles);

  /// <summary>
  ///   Convert <inheritdoc cref="User" /> to JSON
  /// </summary>
  /// <returns>
  ///   <inheritdoc cref="string" /> representing the JSON object of this instance
  /// </returns>
  public string ToJson()
    => JsonSerializer.Serialize(this);

  /// <summary>
  ///   Build a <inheritdoc cref="User" /> from a JSON <inheritdoc cref="string" />
  /// </summary>
  /// <param name="json">JSON value</param>
  /// <returns>
  ///   <inheritdoc cref="User" /> built from the provided JSON
  /// </returns>
  public static User FromJson(string json)
    => JsonSerializer.Deserialize<User>(json)!;

  /// <inheritdoc />
  public override int GetHashCode()
    => HashCode.Combine(Name,
                        Roles);
}
