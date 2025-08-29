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

namespace ArmoniK.Core.Adapters.Nats;

/// <summary>
///   All Allowed option for Nats queue in ArmoniK.
///   Can be set through configuration sources.
/// </summary>
internal class Nats
{
  /// <summary>
  ///   The configuration section name used to bind NATS settings from configuration sources.
  /// </summary>
  public const string SettingSection = nameof(Nats);

  /// <summary>
  ///   The URL of the NATS server to connect to.
  ///   This should include protocol, host, and port, e.g. "nats://localhost:4222".
  /// </summary>
  public string Url { get; set; } = string.Empty;

  /// <summary>
  ///   Acknowledgment deadline in seconds: If a message wasn't acknowledged within this deadline, it will be
  ///   redelivered .
  /// </summary>
  public int AckWait { get; set; } = 120;

  /// <summary>
  ///   Time  in seconds between two modifications of acknowledgment deadline
  /// </summary>
  public int AckExtendDeadlineStep { get; set; } = 60;

  /// <summary>
  ///   Limit on the level of parallelism for operations.
  ///   If DegreeOfParallelism is 0, the number of threads is used as the limit.
  ///   If DegreeOfParallelism is negative, no limit is enforced.
  /// </summary>
  public int DegreeOfParallelism { get; set; }
}
