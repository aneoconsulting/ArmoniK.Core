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

using ArmoniK.Core.Common.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class FileExtTests
{
  [SetUp]
  public void SetUp()
  {
    folder_ = Path.Combine(Path.GetTempPath(),
                           $"ArmoniK.FileExt.{Guid.NewGuid()}");
    Directory.CreateDirectory(folder_);
  }

  [TearDown]
  public void TearDown()
    => Directory.Delete(folder_,
                        true);

  private string folder_ = "";

  [Test]
  public void DriveOfShouldBeTheLongestMountHoldingThePath()
  {
    var drive = FileExt.DriveOf(folder_);
    var root = drive.RootDirectory.FullName.TrimEnd(Path.DirectorySeparatorChar,
                                                    Path.AltDirectorySeparatorChar);

    Assert.That(Path.GetFullPath(folder_),
                Does.StartWith(root));
    // no other mount holding the path has a longer root
    Assert.That(DriveInfo.GetDrives()
                         .Select(d => d.RootDirectory.FullName.TrimEnd(Path.DirectorySeparatorChar,
                                                                       Path.AltDirectorySeparatorChar))
                         .Where(r => r.Length > root.Length && Path.GetFullPath(folder_)
                                                                   .StartsWith(r + Path.DirectorySeparatorChar,
                                                                               StringComparison.Ordinal)),
                Is.Empty);
  }

  [Test]
  public void DriveOfShouldNotMatchAMountPrefixOfAnotherName()
  {
    if (OperatingSystem.IsWindows())
    {
      Assert.Ignore("Unix mount points");
    }

    // "/proc" is a mount point on Linux: "/procfoo" must not be attributed to it
    var drive = FileExt.DriveOf("/procfoo/bar");
    Assert.That(drive.RootDirectory.FullName.TrimEnd('/'),
                Is.Not.EqualTo("/proc"));
  }
}
