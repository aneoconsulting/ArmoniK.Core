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

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Provides extension methods for file operations, including moving and deleting files with additional safety and error
///   handling.
/// </summary>
public static class FileExt
{
  /// <summary>
  ///   Fraction of the space used (0-1) on the file system holding a path.
  /// </summary>
  /// <remarks>
  ///   On Unix, the path itself is measured (statfs), so a folder on its own mount (a tmpfs such as /dev/shm, a volume)
  ///   reports that mount, not the root file system. On Windows, the drive of the path is measured.
  /// </remarks>
  /// <param name="path">Path of a file or folder.</param>
  /// <returns>
  ///   The fraction of the space used, or <see langword="null" /> when the file system holding <paramref name="path" />
  ///   cannot be measured (missing or inaccessible path).
  /// </returns>
  public static double? UsedSpaceFraction(string path)
  {
    try
    {
      var drive = new DriveInfo(path);
      return (double)(drive.TotalSize - drive.AvailableFreeSpace) / drive.TotalSize;
    }
    // DriveNotFoundException for a missing path, DirectoryNotFoundException for a path under a file,
    // UnauthorizedAccessException for a path that cannot be traversed
    catch (Exception e) when (e is IOException or UnauthorizedAccessException)
    {
      return null;
    }
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
