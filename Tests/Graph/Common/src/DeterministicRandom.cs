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

namespace ArmoniK.Samples.Graph.Common;

/// <summary>
///   Small pseudo-random number generator (xoshiro256**, seeded with SplitMix64).
/// </summary>
/// <remarks>
///   <see cref="System.Random" /> is not used on purpose: its algorithm is not guaranteed to be stable across .NET
///   versions, while the graphs must be identical on every machine, for every run and on every retry of a task.
/// </remarks>
public sealed class DeterministicRandom
{
  private ulong s0_;
  private ulong s1_;
  private ulong s2_;
  private ulong s3_;

  /// <summary>
  ///   Creates a generator whose whole sequence is determined by <paramref name="seed" />
  /// </summary>
  /// <param name="seed">Seed of the generator</param>
  public DeterministicRandom(ulong seed)
  {
    var sm = seed;
    s0_ = StableHash.SplitMix64(ref sm);
    s1_ = StableHash.SplitMix64(ref sm);
    s2_ = StableHash.SplitMix64(ref sm);
    s3_ = StableHash.SplitMix64(ref sm);
  }

  /// <summary>
  ///   Creates a generator from its raw state, used to check the reference values of xoshiro256**
  /// </summary>
  /// <param name="s0">First word of the state</param>
  /// <param name="s1">Second word of the state</param>
  /// <param name="s2">Third word of the state</param>
  /// <param name="s3">Fourth word of the state</param>
  internal DeterministicRandom(ulong s0,
                               ulong s1,
                               ulong s2,
                               ulong s3)
  {
    s0_ = s0;
    s1_ = s1;
    s2_ = s2;
    s3_ = s3;
  }

  /// <summary>
  ///   Next 64 random bits
  /// </summary>
  /// <returns>
  ///   A uniformly distributed 64 bits integer
  /// </returns>
  public ulong NextUInt64()
  {
    var result = RotateLeft(s1_ * 5,
                            7) * 9;
    var t = s1_ << 17;

    s2_ ^= s0_;
    s3_ ^= s1_;
    s1_ ^= s2_;
    s0_ ^= s3_;
    s2_ ^= t;
    s3_ = RotateLeft(s3_,
                     45);

    return result;
  }

  /// <summary>
  ///   Uniform double in [0, 1)
  /// </summary>
  /// <returns>
  ///   A double with 53 random bits
  /// </returns>
  public double NextDouble()
    => (NextUInt64() >> 11) * (1.0 / (1UL << 53));

  /// <summary>
  ///   Uniform double in (0, 1], safe to use with a logarithm
  /// </summary>
  /// <returns>
  ///   A double in (0, 1]
  /// </returns>
  public double NextDoubleNonZero()
    => 1.0 - NextDouble();

  /// <summary>
  ///   Uniform integer in [0, <paramref name="maxExclusive" />)
  /// </summary>
  /// <param name="maxExclusive">Exclusive upper bound, must be positive</param>
  /// <returns>
  ///   The drawn integer
  /// </returns>
  public int NextInt(int maxExclusive)
  {
    ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxExclusive);
    var bound = (ulong)maxExclusive;
    // Rejection sampling to avoid the modulo bias
    var threshold = (0UL - bound) % bound;
    while (true)
    {
      var r = NextUInt64();
      if (r >= threshold)
      {
        return (int)(r % bound);
      }
    }
  }

  /// <summary>
  ///   Draws a standard normal variable (Box-Muller transform)
  /// </summary>
  /// <returns>
  ///   A sample of N(0, 1)
  /// </returns>
  public double NextStandardNormal()
  {
    var u1 = NextDoubleNonZero();
    var u2 = NextDouble();
    return Math.Sqrt(-2.0 * Math.Log(u1)) * Math.Cos(2.0 * Math.PI * u2);
  }

  /// <summary>
  ///   Fills the buffer with random bytes
  /// </summary>
  /// <param name="buffer">Buffer to fill</param>
  public void NextBytes(Span<byte> buffer)
  {
    var i = 0;
    while (i < buffer.Length)
    {
      var r = NextUInt64();
      for (var b = 0; b < 8 && i < buffer.Length; b++, i++)
      {
        buffer[i] =   (byte)r;
        r         >>= 8;
      }
    }
  }

  private static ulong RotateLeft(ulong x,
                                  int   k)
    => (x << k) | (x >> (64 - k));
}
