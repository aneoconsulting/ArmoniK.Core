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
///   Configuration for <see cref="Stream.Worker.WorkerStreamHandler" />
/// </summary>
public class InitWorker
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(InitWorker);

  /// <summary>
  ///   Number of times the worker should retry a failed check before giving up.
  /// </summary>
  public int WorkerCheckRetries { get; set; } = 10;

  /// <summary>
  ///   Delay duration between each retry attempt.
  /// </summary>
  public TimeSpan WorkerCheckDelay { get; set; } = TimeSpan.FromSeconds(2);
}
