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

using System;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Utils;

public static class LoggerExt
{
  /// <summary>
  ///   Logs information about the given type
  /// </summary>
  /// <param name="logger">Logger used to print the information</param>
  /// <param name="type">The type to print information about</param>
  /// <param name="logLevel">The level used to print the information</param>
  public static void LogVersion(this ILogger logger,
                                Type         type,
                                LogLevel     logLevel = LogLevel.Information)
    => logger.Log(logLevel,
                  "Version of {class} is {version} ({qualifiedName})",
                  type.FullName,
                  type.Assembly.GetName()
                      .Version,
                  type.AssemblyQualifiedName);
}
