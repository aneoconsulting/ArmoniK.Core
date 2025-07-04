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

// ReSharper disable once CheckNamespace

namespace ArmoniK.Api.gRPC.V1;

/// <summary>
///   Provides configuration constants for payload data transfer in the gRPC API.
/// </summary>
public static class PayloadConfiguration
{
  /// <summary>
  ///   The maximum size in bytes for a single payload chunk.
  /// </summary>
  /// <remarks>
  ///   When transferring large payloads over gRPC, data is split into chunks to
  ///   optimize network transmission. This constant defines the maximum size
  ///   for each individual chunk (approximately 82KB).
  /// </remarks>
  public const int MaxChunkSize = 84000;
}
