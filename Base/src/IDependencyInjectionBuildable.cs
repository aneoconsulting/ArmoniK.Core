// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Base;

/// <summary>
///   Interface to build object for Adapters through Dependency Injection
/// </summary>
public interface IDependencyInjectionBuildable
{
  /// <summary>
  ///   Initialize object through Dependency Injection
  /// </summary>
  /// <param name="serviceCollection">Collection of service descriptors</param>
  /// <param name="configuration">Access to application configuration</param>
  /// <param name="logger">Instance of logger to produce logs during object initialization</param>
  void Build(IServiceCollection   serviceCollection,
             ConfigurationManager configuration,
             ILogger              logger);
}
