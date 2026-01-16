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

using System.Collections.Generic;

namespace ArmoniK.Core.Utils;

/// <summary>
///   Comparer for byte[] instances
/// </summary>
public class ByteArrayComparer : IEqualityComparer<byte[]>
{
  /// <summary>
  ///   Compares 2 byte[] instances
  /// </summary>
  /// <param name="x">First byte[] instance</param>
  /// <param name="y">Second byte[] instance</param>
  /// <returns>true when the 2 instances are equal, false otherwise</returns>
  public bool Equals(byte[]? x,
                     byte[]? y)
  {
    if (ReferenceEquals(x,
                        y))
    {
      return true;
    }

    if (x == null || y == null)
    {
      return false;
    }

    if (x.Length != y.Length)
    {
      return false;
    }

    return x.SequenceEqual(y);
  }

  /// <summary>
  ///   Compute a hash code for a byte[] instance
  /// </summary>
  /// <param name="obj">The byte[] instance</param>
  /// <returns>The hash code</returns>
  public int GetHashCode(byte[]? obj)
  {
    if (obj == null)
    {
      return 0;
    }

    unchecked
    {
      var hash = 17;
      foreach (var b in obj)
      {
        hash = hash * 31 + b;
      }

      return hash;
    }
  }
}
