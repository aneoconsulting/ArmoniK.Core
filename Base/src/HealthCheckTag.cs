// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

namespace ArmoniK.Core.Base;

/// <summary>
///   Tags to filter the kind of health check
/// </summary>
public enum HealthCheckTag
{
  /// <summary>
  ///   For a health check that determines the status of a class exposing health check during its initialization.
  /// </summary>
  Startup,

  /// <summary>
  ///   For a health check that determines the status of a class exposing health check during its execution.
  /// </summary>
  Liveness,

  /// <summary>
  ///   For a health check that determines if a class exposing health check can accept workload.
  /// </summary>
  Readiness,
}
