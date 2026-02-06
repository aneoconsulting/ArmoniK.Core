// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using JetBrains.Annotations;

namespace ArmoniK.Core.Tests.UploadBench.Options;

/// <summary>
///   Class containing options for UploadBench
/// </summary>
[PublicAPI]
public class UploadBench
{
  /// <summary>
  ///   Name of the section in dotnet options
  /// </summary>
  public const string SettingSection = nameof(UploadBench);

  /// <summary>
  ///   Size of the gRPC messages used for the upload
  /// </summary>
  public long MessageSize { get; set; } = 80000;

  /// <summary>
  ///   Total size of the results that should be uploaded
  /// </summary>
  public long ResultSize { get; set; } = 5 * 1024 * 1024;

  /// <summary>
  ///   Number of results to upload
  /// </summary>
  public int NbResults { get; set; } = 100;

  /// <summary>
  ///   Number of threads to use for the upload
  /// </summary>
  public int NbThreads { get; set; } = 1;

  /// <summary>
  ///   Number of repeats
  /// </summary>
  public int Repeats { get; set; } = 10;
}
