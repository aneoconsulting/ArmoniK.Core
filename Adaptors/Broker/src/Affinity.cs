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
                                  byte   TotalSize);

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

  private static readonly ulong[] Thresholds =
  [
    4_294_967_296UL, 4_820_937_788UL, 5_411_319_705UL, 6_074_001_000UL, 6_817_835_604UL, 7_652_761_717UL,
  ];

  private static ulong Fnv1A64(ReadOnlySpan<byte> bytes)
  {
    var h = 0xcbf29ce484222325UL;
    foreach (var b in bytes)
    {
      h ^= b;
      h =  unchecked(h * 0x100000001b3UL);
    }

    return h;
  }

  private static ulong Fmix64(ulong k)
  {
    unchecked
    {
      k ^= k >> 33;
      k *= 0xff51afd7ed558ccdUL;
      k ^= k >> 33;
      k *= 0xc4ceb9fe1a85ec53UL;
      k ^= k >> 33;
      return k;
    }
  }

  /// <summary>
  ///   32-bit hash of a data identifier.
  /// </summary>
  public static uint Hash(string id)
    => unchecked((uint)Fmix64(Fnv1A64(Encoding.UTF8.GetBytes(id))));

  /// <summary>
  ///   Logarithmic encoding of a size, six steps per octave; never 0.
  /// </summary>
  public static byte Encode(ulong size)
  {
    var v = size == ulong.MaxValue
              ? size
              : size + 1;
    var e = 63 - BitOperations.LeadingZeroCount(v);
    var f = ((UInt128)v << 32) >> e;
    var k = Thresholds.Count(t => t <= f);
    return (byte)Math.Min(255,
                          6 * e + k);
  }

  /// <summary>
  ///   Selects the affinity structure of a task from its dependencies (identifier, size in bytes).
  ///   Returns null when the task has no dependency.
  /// </summary>
  public static AffinityData? Select(IEnumerable<(string Id, ulong Size)> dependencies)
  {
    var seen = new HashSet<string>(StringComparer.Ordinal);
    var all  = new List<(string Id, ulong Size, uint Hash)>();
    foreach (var (id, size) in dependencies)
    {
      if (seen.Add(id))
      {
        all.Add((id, size, Hash(id)));
      }
    }

    if (all.Count == 0)
    {
      return null;
    }

    var total = all.Aggregate(0UL,
                              (acc,
                               d) => d.Size > ulong.MaxValue - acc
                                       ? ulong.MaxValue
                                       : acc + d.Size);

    var bySize = all.OrderByDescending(d => d.Size)
                    .ThenBy(d => d.Hash)
                    .ThenBy(d => d.Id,
                            StringComparer.Ordinal)
                    .ToList();
    var taken = bySize.Take(SizeHalf)
                      .ToList();
    var rest = bySize.Skip(SizeHalf)
                     .OrderBy(d => d.Hash)
                     .ThenBy(d => d.Id,
                             StringComparer.Ordinal)
                     .Take(Slots - taken.Count);
    taken.AddRange(rest);

    return new AffinityData(taken.Select(d => d.Hash)
                                 .ToArray(),
                            taken.Select(d => Encode(d.Size))
                                 .ToArray(),
                            (ushort)Math.Min(all.Count,
                                             ushort.MaxValue),
                            Encode(total));
  }
}
