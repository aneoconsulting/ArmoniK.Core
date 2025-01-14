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

using ArmoniK.Core.Base.DataStructures;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Task options place holder for dynamic conversions
/// </summary>
[UsedImplicitly]
public record TaskOptionsHolder
{
  /// <inheritdoc cref="TaskOptions.ApplicationName" />
  public string ApplicationName { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskOptions.ApplicationNamespace" />
  public string ApplicationNamespace { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskOptions.ApplicationService" />
  public string ApplicationService { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskOptions.ApplicationVersion" />
  public string ApplicationVersion { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskOptions.EngineType" />
  public string EngineType { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskOptions.PartitionId" />
  public string PartitionId { get; set; } = string.Empty;

  /// <inheritdoc cref="TaskOptions.MaxRetries" />
  public int MaxRetries { get; set; }

  /// <inheritdoc cref="TaskOptions.Priority" />
  public int Priority { get; set; }

  /// <inheritdoc cref="TaskOptions.MaxDuration" />
  public TimeSpan MaxDuration { get; set; }

  /// <inheritdoc cref="TaskOptions.Options" />
  public IDictionary<string, string> Options { get; set; } = new Dictionary<string, string>();
}
