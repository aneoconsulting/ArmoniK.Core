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
using System.Linq;

using NUnit.Framework;

namespace ArmoniK.Samples.Graph.Common.Tests;

[TestFixture(TestOf = typeof(Distribution))]
public class DistributionTest
{
  [TestCase("const:5",
            "const:5")]
  [TestCase("5",
            "const:5")]
  [TestCase("uniform:5:20",
            "uniform:5:20")]
  [TestCase(" Normal:10:2 ",
            "normal:10:2")]
  [TestCase("lognormal:100:1:max=5000",
            "lognormal:100:1:max=5000")]
  [TestCase("exponential:3:min=1",
            "exponential:3:min=1")]
  [TestCase("poisson:2.5:min=0:max=10",
            "poisson:2.5:min=0:max=10")]
  public void ParseShouldRoundTrip(string text,
                                   string expected)
  {
    var distribution = Distribution.Parse(text);
    Assert.That(distribution.ToString(),
                Is.EqualTo(expected));
    Assert.That(Distribution.Parse(distribution.ToString())
                            .ToString(),
                Is.EqualTo(expected));
  }

  [TestCase("")]
  [TestCase("foo:1")]
  [TestCase("const")]
  [TestCase("const:1:2")]
  [TestCase("uniform:5")]
  [TestCase("uniform:20:5")]
  [TestCase("normal:1:-1")]
  [TestCase("lognormal:0:1")]
  [TestCase("exponential:0")]
  [TestCase("poisson:-1")]
  [TestCase("const:1:min=5:max=2")]
  [TestCase("const:1:avg=2")]
  [TestCase("const:abc")]
  public void ParseShouldRejectInvalid(string text)
    => Assert.That(() => Distribution.Parse(text),
                   Throws.InstanceOf<FormatException>());

  [Test]
  public void BoundsShouldClamp()
  {
    var random = new DeterministicRandom(42);
    Assert.That(Distribution.Parse("const:10:max=3")
                            .Sample(random),
                Is.EqualTo(3));
    Assert.That(Distribution.Parse("const:-10:min=-2")
                            .Sample(random),
                Is.EqualTo(-2));

    var bounded = Distribution.Parse("normal:100:1000:min=50:max=60");
    var samples = Enumerable.Range(0,
                                   1000)
                            .Select(_ => bounded.Sample(random))
                            .ToList();
    Assert.That(samples,
                Has.All.InRange(50,
                                60));
    Assert.That(samples,
                Has.Some.EqualTo(50));
    Assert.That(samples,
                Has.Some.EqualTo(60));
  }

  [Test]
  public void CountsShouldBeRoundedAndClamped()
  {
    var random = new DeterministicRandom(1);
    Assert.That(Distribution.Parse("const:2.5")
                            .SampleCount(random,
                                         0),
                Is.EqualTo(3));
    Assert.That(Distribution.Parse("const:2.4")
                            .SampleCount(random,
                                         0),
                Is.EqualTo(2));
    Assert.That(Distribution.Parse("const:-3")
                            .SampleCount(random,
                                         0),
                Is.EqualTo(0));
    Assert.That(Distribution.Parse("const:0")
                            .SampleCount(random,
                                         1),
                Is.EqualTo(1));
    Assert.That(Distribution.Parse("normal:-5:1")
                            .SampleNonNegative(random),
                Is.EqualTo(0));
  }

  [Test]
  public void UniformCountsShouldCoverAllIntegersEvenly()
  {
    var random       = new DeterministicRandom(7);
    var distribution = Distribution.Parse("uniform:5:8");
    var counts = Enumerable.Range(0,
                                  40000)
                           .Select(_ => distribution.SampleCount(random,
                                                                 1))
                           .GroupBy(c => c)
                           .ToDictionary(g => g.Key,
                                         g => g.Count());

    Assert.That(counts.Keys.Order(),
                Is.EqualTo(Enumerable.Range(5,
                                            4)));
    Assert.That(counts.Values,
                Has.All.InRange(9000,
                                11000));
  }

  [TestCase("uniform:0:10",
            5)]
  [TestCase("normal:10:2",
            10)]
  [TestCase("exponential:3",
            3)]
  [TestCase("poisson:4",
            4)]
  [TestCase("poisson:100",
            100)]
  public void MeanShouldMatch(string text,
                              double mean)
  {
    var random       = new DeterministicRandom(3);
    var distribution = Distribution.Parse(text);
    var average = Enumerable.Range(0,
                                   20000)
                            .Average(_ => distribution.Sample(random));
    Assert.That(average,
                Is.EqualTo(mean)
                  .Within(5)
                  .Percent);
  }

  [Test]
  public void LogNormalMedianShouldMatch()
  {
    var random       = new DeterministicRandom(5);
    var distribution = Distribution.Parse("lognormal:100:1");
    var samples = Enumerable.Range(0,
                                   20001)
                            .Select(_ => distribution.Sample(random))
                            .Order()
                            .ToList();
    Assert.That(samples[10000],
                Is.EqualTo(100)
                  .Within(5)
                  .Percent);
  }
}
