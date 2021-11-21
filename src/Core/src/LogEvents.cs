// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Linq;
using System;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core
{
  [PublicAPI]
  public static class LogEvents
  {
    public static readonly EventId StartMessageProcessing = new(800, "Start message processing");
  }

  public static class LoggerExt
  {
    public static IDisposable BeginNamedScope(this ILogger logger,
                                              string       name, 
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

  }
}
