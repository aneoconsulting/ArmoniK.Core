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
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Numerics;
using System.Text;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Affinity structure of a task, as defined in <c>Broker/docs/protocol.md</c> §8.
///   Must reproduce <c>Broker/conformance/affinity.json</c> exactly.
/// </summary>
/// <param name="Hashes">Hashes of the selected dependencies</param>
/// <param name="Sizes">Encoded sizes of the selected dependencies</param>
/// <param name="DepCount">Number of distinct dependencies, saturated</param>
/// <param name="TotalSize">Encoded sum of all dependency sizes</param>
public sealed record AffinityData(uint[] Hashes,
                                  byte[] Sizes,
                                  ushort DepCount,
                                  byte   TotalSize)
{
  /// <summary>
  ///   <see cref="Sizes" /> as numbers for the JSON wire format, which would encode a byte array in base64.
  /// </summary>
  public int[] WireSizes()
    => Array.ConvertAll(Sizes,
                        s => (int)s);
}

/// <summary>
///   Reference implementation of the affinity computation, shared with the Rust server through the conformance vectors.
/// </summary>
public static class Affinity
{
  /// <summary>
  ///   Number of dependencies kept.
  /// </summary>
  public const int Slots = 8;

  private const int SizeHalf = 4;

  /// <summary>
  ///   32-bit hash of a data identifier: XXH32 of its UTF-8 bytes, seed 0.
  /// </summary>
  public static uint Hash(string id)
  {
    var max = Encoding.UTF8.GetMaxByteCount(id.Length);
    Span<byte> buffer = max <= 256
                   ? stackalloc byte[max]
                   : new byte[max];
    var length = Encoding.UTF8.GetBytes(id,
                                        buffer);
    return XxHash32.HashToUInt32(buffer[..length]);
  }

  /// <summary>
  ///   Logarithmic encoding of <c>size + 1</c> as a minifloat: 4 times its exponent plus the two bits
  ///   after its leading one, 4 steps per octave, plus 1; never 0.
  /// </summary>
  public static byte Encode(ulong size)
  {
    var v = size == ulong.MaxValue
              ? size
              : size + 1;
    var e = 63 - BitOperations.LeadingZeroCount(v);
    var m = (int)((e >= 2
                     ? v >> (e - 2)
                     : v << (2 - e)) & 3);
    return (byte)Math.Min(255,
                          1 + 4 * e + m);
  }

  /// <summary>
  ///   Selects the affinity structure from data sizes as ArmoniK stores them, where a negative size
  ///   is not a real one and counts as empty. Returns null when there is no data.
  /// </summary>
  public static AffinityData? Select(IEnumerable<(string Id, long Size)> data)
    => Select(data.Select(d => (d.Id, (ulong)Math.Max(0,
                                                      d.Size))));

  /// <summary>
  ///   Selects the affinity structure of a task from its dependencies (identifier, size in bytes).
  ///   Returns null when the task has no dependency.
  /// </summary>
  public static AffinityData? Select(IEnumerable<(string Id, ulong Size)> dependencies)
  {
    // The first size of an identifier wins; largest first, ties broken by hash then identifier.
    var all = dependencies.DistinctBy(d => d.Id,
                                      StringComparer.Ordinal)
                          .Select(d => (d.Id, d.Size, Hash: Hash(d.Id)))
                          .OrderByDescending(d => d.Size)
                          .ThenBy(d => d.Hash)
                          .ThenBy(d => d.Id,
                                  StringComparer.Ordinal)
                          .ToList();
    if (all.Count == 0)
    {
      return null;
    }

    var total = all.Aggregate(0UL,
                              (acc,
                               d) => d.Size > ulong.MaxValue - acc
                                       ? ulong.MaxValue
                                       : acc + d.Size);

    // The largest dependencies, then the smallest hashes among the others: a sample that tasks
    // sharing most of their dependencies have in common.
    var taken = all.Take(SizeHalf)
                   .Concat(all.Skip(SizeHalf)
                              .OrderBy(d => d.Hash)
                              .ThenBy(d => d.Id,
                                      StringComparer.Ordinal)
                              .Take(Slots - SizeHalf))
                   .ToList();

    return new AffinityData(taken.Select(d => d.Hash)
                                 .ToArray(),
                            taken.Select(d => Encode(d.Size))
                                 .ToArray(),
                            (ushort)Math.Min(all.Count,
                                             ushort.MaxValue),
                            Encode(total));
  }
}
