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

namespace ArmoniK.Samples.CrashingWorker.Client.Options;

/// <summary>
///   Class containing options for CrashingWorkerOptions
/// </summary>
public class CrashingWorkerOptions
{
  /// <summary>
  ///   Name of the section in dotnet options
  /// </summary>
  public const string SettingSection = nameof(CrashingWorkerOptions);

  /// <summary>
  ///   Partition in which to submit the tasks
  /// </summary>
  public string Partition { get; set; } = string.Empty;

  /// <summary>
  ///   Type of the crash to simulate
  /// </summary>
  /// <remarks>
  ///   Possible values:
  ///   - success
  ///   - error
  ///   - exception
  ///   - rpc-cancelled
  ///   - rpc-unknown
  ///   - rpc-invalid-argument
  ///   - rpc-deadline-exceeded
  ///   - rpc-not-found
  ///   - rpc-already-exists
  ///   - rpc-permission-denied
  ///   - rpc-resource-exhausted
  ///   - rpc-failed-precondition
  ///   - rpc-aborted
  ///   - rpc-out-of-range
  ///   - rpc-unimplemented
  ///   - rpc-internal
  ///   - rpc-unavailable
  ///   - rpc-data-loss
  ///   - rpc-unauthenticated
  ///   - exit
  ///   - kill
  ///   - crash
  /// </remarks>
  public string Type { get; set; } = string.Empty;

  /// <summary>
  ///   Number of tasks to submit
  /// </summary>
  public int NbTasks { get; set; } = 10;

  /// <summary>
  ///   Number of allowed retries
  /// </summary>
  public int Retry { get; set; } = 3;
}
