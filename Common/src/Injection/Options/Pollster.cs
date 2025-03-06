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

namespace ArmoniK.Core.Common.Injection.Options;

/// <summary>
///   Configuration for <see cref="Common.Pollster.Pollster" />
/// </summary>
public class Pollster
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(Pollster);

  /// <summary>
  ///   Grace delay before the pollster cancels the task and put the message back into the queue
  /// </summary>
  public TimeSpan GraceDelay { get; set; } = TimeSpan.FromSeconds(5);

  /// <summary>
  ///   Timeout passed to IHost.StopAsync that will shutdown the application
  /// </summary>
  public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromDays(1);

  /// <summary>
  ///   Maximum number of consecutive errors allowed in the pollster before it crashes
  ///   Negative values disable the check
  /// </summary>
  public int MaxErrorAllowed { get; set; } = 5;

  /// <summary>
  ///   Timeout before releasing the current acquired task and acquiring a new one
  ///   This happens in parallel of the execution of another task
  /// </summary>
  public TimeSpan TimeoutBeforeNextAcquisition { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Number of acquisitions to try during the processing of a previous task.
  ///   If the processing task is still running after that many acquisitions,
  ///   the Agent will stop acquiring tasks until the processing task has finished.
  /// </summary>
  public int NbAcquisitionRetry { get; set; } = 3;

  /// <summary>
  ///   Shared folder between Agent and Worker
  /// </summary>
  public string SharedCacheFolder { get; set; } = "/cache/shared";

  /// <summary>
  ///   Internal cache for data
  /// </summary>
  public string InternalCacheFolder { get; set; } = "/cache/internal";
}
