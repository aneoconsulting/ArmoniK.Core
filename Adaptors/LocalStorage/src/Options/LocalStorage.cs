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

namespace ArmoniK.Core.Adapters.LocalStorage.Options;

public class LocalStorage
{
  public const string SettingSection = nameof(LocalStorage);

  internal static readonly LocalStorage Default = new();

  public string Path
  {
    get;
    [UsedImplicitly]
    set;
  } = System.IO.Path.Combine(System.IO.Path.GetTempPath(),
                             "ArmoniK");

  public int ChunkSize
  {
    get;
    [UsedImplicitly]
    init;
  } = 64 * 1024;

  /// <summary>
  ///   If larger than 0, specify how many characters of the keys are used as the folder prefix.
  /// </summary>
  /// <example>
  ///   If <see cref="Path" /> is "/tmp/ArmoniK", <see cref="SplitPathAt" /> is 3, and key is
  ///   "123e4567-e89b-12d3-a456-426614174000":
  ///   its full path will be "/tmp/ArmoniK/123.d/e4567-e89b-12d3-a456-426614174000"
  /// </example>
  public int SplitPathAt
  {
    get;
    [UsedImplicitly]
    init;
  } = 3;

  /// <summary>
  ///   Number of tasks to be used in parallel execution (deletes, metadata lookups).
  /// </summary>
  public int DegreeOfParallelism { get; set; }

  /// <summary>
  ///   Number of files an object is striped over, written and read concurrently.
  ///   With more than one, the object is cut into stripes of <see cref="StripeSize" /> bytes dealt round-robin to
  ///   the files "key.0" to "key.{StripeCount - 1}"; on a parallel file system, several streams reach a higher
  ///   throughput than one. 1 (the default) or less keeps one file per object.
  /// </summary>
  /// <remarks>
  ///   The layout is part of the key of the object, so objects written with other settings are still read correctly.
  /// </remarks>
  public int StripeCount
  {
    get;
    [UsedImplicitly]
    init;
  } = 1;

  /// <summary>
  ///   Size in bytes of the stripes of an object when <see cref="StripeCount" /> is larger than 1.
  ///   0 or less is replaced by the default (4 MiB).
  /// </summary>
  public int StripeSize
  {
    get;
    [UsedImplicitly]
    init;
  } = 4 * 1024 * 1024;
}
