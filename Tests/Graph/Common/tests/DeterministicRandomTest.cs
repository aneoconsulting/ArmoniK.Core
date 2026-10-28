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

using System.Linq;

using NUnit.Framework;

namespace ArmoniK.Samples.Graph.Common.Tests;

/// <summary>
///   Checks the generator against values published by independent implementations of the reference algorithms
/// </summary>
[TestFixture(TestOf = typeof(DeterministicRandom))]
public class DeterministicRandomTest
{
  /// <summary>
  ///   Reference values of SplitMix64 seeded with 1477776061723855037, produced with the reference implementation
  ///   (https://prng.di.unimi.it/splitmix64.c) and published in the tests of the rand_xoshiro crate:
  ///   https://github.com/rust-random/rngs/blob/master/rand_xoshiro/src/splitmix64.rs
  /// </summary>
  private static readonly ulong[] SplitMix64Reference =
  [
    1985237415132408290UL, 2979275885539914483UL, 13511426838097143398UL, 8488337342461049707UL, 15141737807933549159UL, 17093170987380407015UL,
    16389528042912955399UL, 13177319091862933652UL, 10841969400225389492UL, 17094824097954834098UL, 3336622647361835228UL, 9678412372263018368UL,
    11111587619974030187UL, 7882215801036322410UL, 5709234165213761869UL, 7799681907651786826UL, 4616320717312661886UL, 4251077652075509767UL, 7836757050122171900UL,
    5054003328188417616UL, 12919285918354108358UL, 16477564761813870717UL, 5124667218451240549UL, 18099554314556827626UL, 7603784838804469118UL, 6358551455431362471UL,
    3037176434532249502UL, 3217550417701719149UL, 9958699920490216947UL, 5965803675992506258UL, 12000828378049868312UL, 12720568162811471118UL, 245696019213873792UL,
    8351371993958923852UL, 14378754021282935786UL, 5655432093647472106UL, 5508031680350692005UL, 8515198786865082103UL, 6287793597487164412UL, 14963046237722101617UL,
    3630795823534910476UL, 8422285279403485710UL, 10554287778700714153UL, 10871906555720704584UL, 8659066966120258468UL, 9420238805069527062UL, 10338115333623340156UL,
    13514802760105037173UL, 14635952304031724449UL, 15419692541594102413UL,
  ];

  /// <summary>
  ///   Reference values of xoshiro256** from the state {1, 2, 3, 4}, produced with the reference implementation
  ///   (https://prng.di.unimi.it/xoshiro256starstar.c) and published in the tests of the rand_xoshiro crate:
  ///   https://github.com/rust-random/rngs/blob/master/rand_xoshiro/src/xoshiro256starstar.rs
  /// </summary>
  private static readonly ulong[] Xoshiro256StarStarReference =
  [
    11520UL, 0UL, 1509978240UL, 1215971899390074240UL, 1216172134540287360UL, 607988272756665600UL, 16172922978634559625UL, 8476171486693032832UL,
    10595114339597558777UL, 2904607092377533576UL,
  ];

  [Test]
  public void SplitMix64ShouldMatchReferenceValues()
  {
    var state = 1477776061723855037UL;
    var values = SplitMix64Reference.Select(_ => StableHash.SplitMix64(ref state))
                                    .ToArray();

    Assert.That(values,
                Is.EqualTo(SplitMix64Reference));
  }

  [Test]
  public void Xoshiro256StarStarShouldMatchReferenceValues()
  {
    var random = new DeterministicRandom(1,
                                         2,
                                         3,
                                         4);
    var values = Xoshiro256StarStarReference.Select(_ => random.NextUInt64())
                                            .ToArray();

    Assert.That(values,
                Is.EqualTo(Xoshiro256StarStarReference));
  }

  /// <summary>
  ///   Reference value from the tests of the PHP implementation: the 1001st output from a state whose four words are the
  ///   little-endian bytes 01 02 03 04 05 06 07 08, printed as little-endian bytes "90a025df9300cfd1":
  ///   https://github.com/php/php-src/blob/master/ext/random/tests/02_engine/xoshiro256starstar_seed.phpt
  /// </summary>
  [Test]
  public void Xoshiro256StarStarShouldMatchReferenceValueAfterManyDraws()
  {
    const ulong word = 0x0807060504030201UL;
    var random = new DeterministicRandom(word,
                                         word,
                                         word,
                                         word);
    for (var i = 0; i < 1000; i++)
    {
      random.NextUInt64();
    }

    Assert.That(random.NextUInt64(),
                Is.EqualTo(0xd1cf0093df25a090UL));
  }

  [Test]
  public void SeedShouldInitializeStateWithSplitMix64()
  {
    // Seeding recommended by the authors of xoshiro256**: the state is filled with consecutive outputs of SplitMix64
    const ulong seed = 1477776061723855037UL;
    var expected = new DeterministicRandom(SplitMix64Reference[0],
                                           SplitMix64Reference[1],
                                           SplitMix64Reference[2],
                                           SplitMix64Reference[3]);
    var random = new DeterministicRandom(seed);

    Assert.That(Enumerable.Range(0,
                                 100)
                          .Select(_ => random.NextUInt64()),
                Is.EqualTo(Enumerable.Range(0,
                                            100)
                                     .Select(_ => expected.NextUInt64())));
  }
}
