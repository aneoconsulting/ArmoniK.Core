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

using JetBrains.Annotations;

// ReSharper disable InconsistentNaming

namespace ArmoniK.Core.Control.Metrics.Options;

/// <summary>
///   Represents the configuration settings for the metrics exporter.
/// </summary>
[PublicAPI]
public class MetricsExporter
{
  /// <summary>
  ///   The configuration section path used to retrieve settings related to the metrics exporter.
  /// </summary>
  public const string SettingSection = nameof(MetricsExporter);

  /// <summary>
  ///   Gets or sets the metrics to be exported.
  ///   This property defines the specific metrics that the exporter will handle.
  /// </summary>
  public string Metrics { get; set; } = "";

  /// <summary>
  ///   Gets or sets the cache validity duration.
  ///   This defines the time span for which the cached metrics are considered valid.
  /// </summary>
  public TimeSpan CacheValidity { get; set; } = TimeSpan.FromSeconds(5);
}
