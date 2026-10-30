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

using System.Text;

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Hash functions whose values do not depend on the process, the machine or the .NET version
///   (unlike <see cref="string.GetHashCode()" /> that is randomized per process).
/// </summary>
public static class StableHash
{
  private const ulong FnvOffsetBasis = 14695981039346656037UL;
  private const ulong FnvPrime       = 1099511628211UL;

  /// <summary>
  ///   Advances a SplitMix64 state and returns the next value
  /// </summary>
  /// <param name="state">State of the generator, updated</param>
  /// <returns>
  ///   The next pseudo-random value
  /// </returns>
  public static ulong SplitMix64(ref ulong state)
  {
    state += 0x9E3779B97F4A7C15UL;
    return Mix(state);
  }

  /// <summary>
  ///   SplitMix64 finalizer: bijective mixing of the bits of a 64 bits value
  /// </summary>
  /// <param name="z">Value to mix</param>
  /// <returns>
  ///   The mixed value
  /// </returns>
  public static ulong Mix(ulong z)
  {
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
    return z ^ (z >> 31);
  }

  /// <summary>
  ///   FNV-1a 64 bits hash of the UTF-8 bytes of a string
  /// </summary>
  /// <param name="value">String to hash</param>
  /// <returns>
  ///   The hash of the string
  /// </returns>
  public static ulong Fnv1A64(string value)
    => Fnv1A64(FnvOffsetBasis,
               value);

  /// <summary>
  ///   Continues a FNV-1a 64 bits hash with the UTF-8 bytes of a string:
  ///   <c>Fnv1A64(Fnv1A64(a), b) == Fnv1A64(a + b)</c>
  /// </summary>
  /// <param name="hash">Hash of the prefix</param>
  /// <param name="value">String appended to the prefix</param>
  /// <returns>
  ///   The hash of the prefix followed by <paramref name="value" />
  /// </returns>
  public static ulong Fnv1A64(ulong  hash,
                              string value)
  {
    foreach (var b in Encoding.UTF8.GetBytes(value))
    {
      hash ^= b;
      hash *= FnvPrime;
    }

    return hash;
  }

  /// <summary>
  ///   Combines several values into one well mixed hash
  /// </summary>
  /// <param name="values">Values to combine, order matters</param>
  /// <returns>
  ///   The combined hash
  /// </returns>
  public static ulong Combine(params ulong[] values)
  {
    var hash = FnvOffsetBasis;
    foreach (var value in values)
    {
      hash = Mix(hash ^ Mix(value + 0x9E3779B97F4A7C15UL));
    }

    return hash;
  }
}
