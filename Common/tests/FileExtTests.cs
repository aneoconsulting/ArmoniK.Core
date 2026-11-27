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

using ArmoniK.Core.Common.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class FileExtTests
{
  [Test]
  public void UsedSpaceFractionShouldBeNullForAMissingPath()
  {
    if (OperatingSystem.IsWindows())
    {
      Assert.Ignore("Windows measures the drive of the path, which exists");
    }

    Assert.That(FileExt.UsedSpaceFraction(Path.Combine(Path.GetTempPath(),
                                                       $"ArmoniK.FileExt.{Guid.NewGuid()}")),
                Is.Null);
  }

  [Test]
  public void UsedSpaceFractionShouldBeNullForAPathUnderAFile()
  {
    if (OperatingSystem.IsWindows())
    {
      Assert.Ignore("Windows measures the drive of the path, which exists");
    }

    // statfs fails with ENOTDIR, which DriveInfo does not map to DriveNotFoundException
    var file = Path.GetTempFileName();
    try
    {
      Assert.That(FileExt.UsedSpaceFraction(Path.Combine(file,
                                                         "cache")),
                  Is.Null);
    }
    finally
    {
      File.Delete(file);
    }
  }

  [Test]
  public void UsedSpaceFractionShouldMeasureTheFileSystemHoldingThePath()
  {
    const string mount = "/dev/shm";
    if (!OperatingSystem.IsLinux() || !Directory.Exists(mount))
    {
      Assert.Ignore($"Needs {mount}");
    }

    // measured independently of the code under test: DriveInfo of a mount point measures that mount
    var mountUsage = Usage(new DriveInfo(mount));
    var rootUsage  = Usage(new DriveInfo("/"));
    // the test only discriminates when the two file systems differ enough in usage
    Assume.That(Math.Abs(mountUsage - rootUsage),
                Is.GreaterThan(0.02),
                $"{mount} is not a mount distinct from / with a different usage");

    var folder = Path.Combine(mount,
                              $"ArmoniK.FileExt.{Guid.NewGuid()}",
                              "cache");
    Directory.CreateDirectory(folder);
    try
    {
      Assert.That(FileExt.UsedSpaceFraction(folder),
                  Is.EqualTo(mountUsage)
                    .Within(0.01));
    }
    finally
    {
      Directory.Delete(Path.GetDirectoryName(folder)!,
                       true);
    }
  }

  private static double Usage(DriveInfo drive)
    => (double)(drive.TotalSize - drive.AvailableFreeSpace) / drive.TotalSize;
}
