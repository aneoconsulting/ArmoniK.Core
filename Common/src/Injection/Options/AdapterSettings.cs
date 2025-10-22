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

namespace ArmoniK.Core.Common.Injection.Options;

/// <summary>
///   Represents configuration settings for an adapter.
/// </summary>
public class AdapterSettings
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(AdapterSettings);

  /// <summary>
  ///   Absolute file system path of the adapter's plugin assembly in the container
  /// </summary>
  public string AdapterAbsolutePath { get; set; } = "";

  /// <summary>
  ///   Fully qualified name of the adapter class that implements the
  ///   <see cref="ArmoniK.Core.Base.IDependencyInjectionBuildable" /> interface
  /// </summary>
  public string ClassName { get; set; } = "";
}
