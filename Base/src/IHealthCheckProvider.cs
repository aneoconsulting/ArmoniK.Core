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

using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Base;

/// <summary>
///   Represents a simpler health check than <see cref="IHealthCheck" />.
/// </summary>
public interface IHealthCheckProvider
{
  /// <summary>
  ///   Checks the status of a class for the given health check type.
  /// </summary>
  /// <param name="tag">Health check for which the class has to answer.</param>
  /// <returns>
  ///   The result of the check containing the status of the class for the health check type.
  /// </returns>
  Task<HealthCheckResult> Check(HealthCheckTag tag);
}
