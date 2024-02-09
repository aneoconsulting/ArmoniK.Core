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

using System.Reflection;
using System.Runtime.Loader;

namespace ArmoniK.Core.Common.Injection;

/// <summary>
///   Class holding load context for Adapters
/// </summary>
public class AdapterLoadContext : AssemblyLoadContext
{
  private readonly AssemblyDependencyResolver resolver_;

  /// <summary>
  ///   Instantiate a <see cref="AdapterLoadContext" /> with the path to the assembly to be loaded.
  /// </summary>
  /// <param name="assemblyPath">Path to the assembly</param>
  public AdapterLoadContext(string assemblyPath)
    => resolver_ = new AssemblyDependencyResolver(assemblyPath);

  /// <inheritdoc />
  protected override Assembly? Load(AssemblyName assemblyName)
  {
    var assemblyPath = resolver_.ResolveAssemblyToPath(assemblyName);
    return assemblyPath is not null
             ? LoadFromAssemblyPath(assemblyPath)
             : null;
  }

  /// <inheritdoc />
  protected override nint LoadUnmanagedDll(string unmanagedDllName)
  {
    var libraryPath = resolver_.ResolveUnmanagedDllToPath(unmanagedDllName);
    return libraryPath is not null
             ? LoadUnmanagedDllFromPath(libraryPath)
             : nint.Zero;
  }
}
