// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Linq;
using System.Runtime.CompilerServices;

using ArmoniK.Core.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core
{
  public static class LoggerExt
  {
    public static IDisposable BeginNamedScope(this ILogger                        logger,
                                              string                              name, 
                                              params ValueTuple<string, object>[] properties)
    {
      var dictionary = properties.ToDictionary(p => p.Item1, p => p.Item2);
      dictionary[name + ".Scope"] = Guid.NewGuid();
      return logger.BeginScope(dictionary);
    }

    public static IDisposable BeginPropertyScope(this   ILogger                      logger,
                                                 params ValueTuple<string, object>[] properties)
    {
      var dictionary = properties.ToDictionary(p => p.Item1, p => p.Item2);
      return logger.BeginScope(dictionary);
    }

    public static IDisposable LogFunction(this ILogger              logger, 
                                          LogLevel                  level        = LogLevel.Debug,
                                          [CallerMemberName] string functionName = "")
    {
      logger.Log(level, "Entering {functionName}", functionName);

      return Disposable.Create(() => logger.Log(level, "Leaving {functionName}", functionName));
    }

  }
}
