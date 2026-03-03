// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

using System;
using JetBrains.Annotations;

namespace ArmoniK.Core.Adapters.TaskDB.Options;

/// <summary>
///   Configuration settings for connecting to the TaskDB server.
/// </summary>
[PublicAPI]
public class TaskDB
{
  /// <summary>
  ///   The configuration section name.
  /// </summary>
  public const string SettingSection = nameof(TaskDB);

  /// <summary>
  ///   Hostname of the TaskDB server.
  /// </summary>
  public string Host { get; set; } = "localhost";

  /// <summary>
  ///   TCP port of the TaskDB server.
  /// </summary>
  public int Port { get; set; } = 7890;

  /// <summary>
  ///   Minimum delay between polling attempts (used for task watchers).
  /// </summary>
  public TimeSpan PollingDelayMin { get; set; } = TimeSpan.FromSeconds(1);

  /// <summary>
  ///   Maximum delay between polling attempts.
  /// </summary>
  public TimeSpan PollingDelayMax { get; set; } = TimeSpan.FromMinutes(5);

  /// <summary>
  ///   Timeout for individual TCP send/receive operations.
  /// </summary>
  public TimeSpan SocketTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
