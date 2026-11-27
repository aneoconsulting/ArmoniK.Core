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

using System;
using System.IO;
using System.Linq;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Provides extension methods for file operations, including moving and deleting files with additional safety and error
///   handling.
/// </summary>
public static class FileExt
{
  /// <summary>
  ///   Get the file system holding a path: the mounted drive with the longest root that contains the path.
  /// </summary>
  /// <remarks>
  ///   On Unix, <see cref="Path.GetPathRoot(string)" /> is always "/", whatever the mount point of the path.
  /// </remarks>
  /// <param name="path">Path of a file or folder.</param>
  /// <returns>The drive holding <paramref name="path" />.</returns>
  public static DriveInfo DriveOf(string path)
  {
    var fullPath = Path.GetFullPath(path);
    return DriveInfo.GetDrives()
                    .Where(drive => IsInside(fullPath,
                                             drive.RootDirectory.FullName))
                    .MaxBy(drive => drive.RootDirectory.FullName.Length) ?? new DriveInfo(Path.GetPathRoot(fullPath)!);
  }

  private static bool IsInside(string path,
                               string root)
  {
    var trimmed = root.TrimEnd(Path.DirectorySeparatorChar,
                               Path.AltDirectorySeparatorChar);
    return trimmed.Length == 0                                          || path.Equals(trimmed,
                                                                                       StringComparison.Ordinal) || path.StartsWith(trimmed + Path.DirectorySeparatorChar,
                                                                                                                                    StringComparison.Ordinal);
  }

  /// <summary>
  ///   Try moving file from source into destination.
  ///   If destination already exists, it will not be overwritten,
  ///   and source will be deleted.
  /// </summary>
  /// <param name="sourceFilename">The name of the file to move. Can include a relative or absolute path.</param>
  /// <param name="destinationFilename">The new path and name for the file.</param>
  /// <returns>Whether the file has been moved</returns>
  public static bool MoveOrDelete(string sourceFilename,
                                  string destinationFilename)
  {
    try
    {
      File.Move(sourceFilename,
                destinationFilename);

      return true;
    }
    catch (IOException)
    {
      TryDelete(sourceFilename);

      if (!File.Exists(destinationFilename))
      {
        throw;
      }
    }

    return false;
  }

  /// <summary>
  ///   Try deleting a file.
  ///   Do not throw any error in case the file does not exist (eg: already deleted)
  /// </summary>
  /// <param name="path">Path of the file to delete</param>
  /// <returns>Whether the file has been deleted</returns>
  public static bool TryDelete(string path)
  {
    try
    {
      File.Delete(path);

      return true;
    }
    catch (IOException)
    {
      if (File.Exists(path))
      {
        throw;
      }
    }

    return false;
  }
}
