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
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.DynamicLoading;

/// <summary>
///   Resolves assemblies located in the same directory as the requesting assembly or this resolver.
///   Maintains a list of directories where assemblies are likely to be found and attempts to load from them.
/// </summary>
public class CollocatedAssemblyResolver
{
  private static readonly ConcurrentDictionary<string, bool> PathDictionary = new();
  private readonly        ILogger                            logger_;


  /// <summary>
  ///   Initializes a new instance of the <see cref="CollocatedAssemblyResolver" /> class,
  ///   setting up known directories for assembly resolution.
  /// </summary>
  /// <param name="logger">The logger to record debug and warning messages.</param>
  public CollocatedAssemblyResolver(ILogger logger)
  {
    GetLoadDirectories();
    logger_ = logger;
  }

  private static void GetLoadDirectories()
  {
    AppDomain.CurrentDomain.AssemblyLoad += (_,
                                             eventArgs) =>
                                            {
                                              var path = Path.GetDirectoryName(eventArgs.LoadedAssembly.Location);
                                              if (path == null)
                                              {
                                                return;
                                              }

                                              PathDictionary[path] = true;
                                            };

    PathDictionary[Path.GetDirectoryName(Assembly.GetCallingAssembly()
                                                 .Location)!] = true;

    PathDictionary[Path.GetDirectoryName(typeof(CollocatedAssemblyResolver).Assembly.Location)!] = true;
  }

  /// <summary>
  ///   Attempts to resolve an assembly by searching the directories at the requesting assembly's location.
  /// </summary>
  /// <param name="sender">The source of the event (unused).</param>
  /// <param name="args">Contextual information about the assembly to resolve.</param>
  /// <returns>The resolved assembly if found; otherwise, <c>null</c>.</returns>
  public Assembly? AssemblyResolve(object?          sender,
                                   ResolveEventArgs args)
  {
    logger_.LogDebug("RequestingAssembly {RequestingAssembly}",
                     args.RequestingAssembly);

    if (args.RequestingAssembly?.Location is null)
    {
      return null;
    }

    var assemblyName = new AssemblyName(args.Name).Name;

    if (string.IsNullOrEmpty(assemblyName))
    {
      logger_.LogWarning("The assembly to resolve {AssemblyToResolve} does not have a name",
                         args.Name);
      return null;
    }

    var directoryName = Path.GetDirectoryName(args.RequestingAssembly.Location);

    if (!string.IsNullOrEmpty(directoryName))
    {
      PathDictionary[directoryName] = true;
    }

    foreach (var path in PathDictionary.Keys)
    {
      var assemblyPath = Path.Combine(path,
                                      assemblyName + ".dll");

      if (File.Exists(assemblyPath))
      {
        logger_.LogDebug("Loading assembly {Assembly} from {Directory}",
                         assemblyName,
                         path);
        return Assembly.LoadFile(assemblyPath);
      }

      logger_.LogDebug("Assembly {Assembly} not found in {Directory}",
                       assemblyName,
                       path);
    }

    return null;
  }
}
