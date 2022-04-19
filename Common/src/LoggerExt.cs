// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;

using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common;

public static class LoggerExt
{
  public static IDisposable BeginNamedScope(this ILogger                        logger,
                                            string                              name,
                                            params ValueTuple<string, object>[] properties)
  {
    var dictionary = properties.ToDictionary(p => p.Item1,
                                             p => p.Item2);
    dictionary[name + ".Scope"] = Guid.NewGuid();
    return logger.BeginScope(dictionary);
  }

  public static IDisposable BeginPropertyScope(this   ILogger                      logger,
                                               params ValueTuple<string, object>[] properties)
  {
    var dictionary = properties.ToDictionary(p => p.Item1,
                                             p => p.Item2);
    return logger.BeginScope(dictionary);
  }

  public static IDisposable LogFunction(this ILogger              logger,
                                        string                    id            = "",
                                        LogLevel                  level         = LogLevel.Trace,
                                        [CallerMemberName] string functionName  = "",
                                        [CallerFilePath]   string classFilePath = "",
                                        [CallerLineNumber] int    line          = 0)
  {
    if (!logger.IsEnabled(level))
    {
      return Disposable.Create(() =>
                               {
                               });
    }
    var properties = new List<ValueTuple<string, object>>
                     {
                       (nameof(functionName), functionName),
                       (nameof(classFilePath), classFilePath),
                       (nameof(line), line),
                     };
    if (!string.IsNullOrEmpty(id))
    {
      properties.Add(("Id", id));
    }

    var scope = logger.BeginNamedScope($"{classFilePath}.{functionName}",
                                       properties.ToArray());

    logger.Log(level,
               "Entering {classFilePath}.{functionName} - {Id}",
               classFilePath,
               functionName,
               id);

    return Disposable.Create(() =>
                             {
                               logger.Log(level,
                                          "Leaving {classFilePath}.{functionName} - {Id}",
                                          classFilePath,
                                          functionName,
                                          id);
                               scope.Dispose();
                             });
  }
}
