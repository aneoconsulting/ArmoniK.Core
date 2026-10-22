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

[TestFixture(TestOf = typeof(LevelShape))]
public class LevelShapeTest
{
  [TestCase("uniform",
            10,
            3,
            new[]
            {
              4,
              3,
              3,
            })]
  [TestCase("uniform",
            3,
            3,
            new[]
            {
              1,
              1,
              1,
            })]
  [TestCase("increasing",
            9,
            3,
            new[]
            {
              2,
              3,
              4,
            })]
  [TestCase("decreasing",
            9,
            3,
            new[]
            {
              4,
              3,
              2,
            })]
  [TestCase("1,10,100",
            114,
            3,
            new[]
            {
              2,
              11,
              101,
            })]
  [TestCase("1,2",
            11,
            4,
            new[]
            {
              2,
              3,
              3,
              3,
            })]
  [TestCase("1,2,3",
            5,
            2,
            new[]
            {
              2,
              3,
            })]
  public void SplitShouldFollowWeights(string shape,
                                       int    tasks,
                                       int    levels,
                                       int[]  expected)
    => Assert.That(LevelShape.Parse(shape)
                             .Split(tasks,
                                    levels),
                   Is.EqualTo(expected));

  [Test]
  public void SplitShouldGiveAtLeastOneTaskPerLevel([Values("uniform", "increasing", "decreasing", "1,1000")] string shape,
                                                    [Values(1, 2, 7, 100)]                                    int    tasks,
                                                    [Values(1, 2, 5)]                                         int    levels)
  {
    levels = Math.Min(levels,
                      tasks);
    var split = LevelShape.Parse(shape)
                          .Split(tasks,
                                 levels);
    Assert.That(split,
                Has.Length.EqualTo(levels));
    Assert.That(split,
                Has.All.GreaterThanOrEqualTo(1));
    Assert.That(split.Sum(),
                Is.EqualTo(tasks));
  }

  [TestCase("")]
  [TestCase("random")]
  [TestCase("1,0,2")]
  [TestCase("1,-2")]
  [TestCase("1,a")]
  public void ParseShouldRejectInvalid(string text)
    => Assert.That(() => LevelShape.Parse(text),
                   Throws.InstanceOf<FormatException>());

  [TestCase("uniform",
            ParentChoiceKind.Uniform,
            0)]
  [TestCase("local",
            ParentChoiceKind.Local,
            0)]
  [TestCase("zipf:1.5",
            ParentChoiceKind.Zipf,
            1.5)]
  public void ParentChoiceShouldParse(string           text,
                                      ParentChoiceKind kind,
                                      double           exponent)
  {
    var choice = ParentChoice.Parse(text);
    Assert.That(choice.Kind,
                Is.EqualTo(kind));
    Assert.That(choice.Exponent,
                Is.EqualTo(exponent));
    Assert.That(choice.ToString(),
                Is.EqualTo(text));
  }

  [TestCase("zipf")]
  [TestCase("zipf:-1")]
  [TestCase("local:2")]
  [TestCase("nearest")]
  public void ParentChoiceShouldRejectInvalid(string text)
    => Assert.That(() => ParentChoice.Parse(text),
                   Throws.InstanceOf<FormatException>());
}
