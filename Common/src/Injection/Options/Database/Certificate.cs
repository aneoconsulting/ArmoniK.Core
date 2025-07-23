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

using System.Text.Json;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <summary>
///   Placeholder to deserialize certificates for Authentication provided by users
/// </summary>
public record Certificate
{
  /// <summary>
  ///   User associated to certificate
  /// </summary>
  public required string User { get; init; }

  // ReSharper disable once InconsistentNaming
  /// <summary>
  ///   Certificate common name
  /// </summary>
  public required string CN { get; init; }

  /// <summary>
  ///   Certificate fingerprint
  /// </summary>
  public string? Fingerprint { get; init; }

  /// <summary>
  ///   Convert <inheritdoc cref="Certificate" /> to JSON
  /// </summary>
  /// <returns>
  ///   <inheritdoc cref="string" /> representing the JSON object of this instance
  /// </returns>
  public string ToJson()
    => JsonSerializer.Serialize(this);

  /// <summary>
  ///   Build a <inheritdoc cref="Certificate" /> from a JSON <inheritdoc cref="string" />
  /// </summary>
  /// <param name="json">JSON value</param>
  /// <returns>
  ///   <inheritdoc cref="Certificate" /> built from the provided JSON
  /// </returns>
  public static Certificate FromJson(string json)
    => JsonSerializer.Deserialize<Certificate>(json)!;
}
