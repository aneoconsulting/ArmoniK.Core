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

using JetBrains.Annotations;

namespace ArmoniK.Core.Adapters.LocalStorage.Options;

internal class LocalStorage
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
}
