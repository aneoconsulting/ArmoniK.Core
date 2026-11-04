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

using NUnit.Framework;

namespace ArmoniK.Samples.Graph.Common.Tests;

[TestFixture(TestOf = typeof(GraphSettings))]
public class GraphSettingsTest
{
  private static GraphSettings Settings(string taskDuration = "const:100",
                                        string outputSize   = "const:1")
    => GraphSettings.Parse(1,
                           "uniform:5:20",
                           3,
                           "uniform",
                           "uniform:1:4",
                           "uniform",
                           0,
                           taskDuration,
                           outputSize);

  [TestCase("const:100",
            5 * 60)]
  [TestCase("uniform:100:600000",
            16 * 60)]
  [TestCase("uniform:0:1000:min=600000",
            16 * 60)]
  [TestCase("lognormal:100:1:max=1200000",
            31 * 60)]
  public void MaxTaskDurationShouldBoundTheDrawnDurations(string taskDuration,
                                                          int    expectedSeconds)
    => Assert.That(Settings(taskDuration)
                     .MaxTaskDuration,
                   Is.EqualTo(TimeSpan.FromSeconds(expectedSeconds)));

  [Test]
  public void MaxTaskDurationShouldBeNullWithoutUpperBound()
    => Assert.That(Settings("lognormal:100:1")
                     .MaxTaskDuration,
                   Is.Null);

  [Test]
  public void MaxTaskDurationShouldNotOverflow()
    // Durations are drawn as int milliseconds, so the bound is capped at int.MaxValue
    => Assert.That(Settings("const:1e15")
                     .MaxTaskDuration,
                   Is.EqualTo(TimeSpan.FromMilliseconds(int.MaxValue * GraphSettings.MaxDurationFactor) + GraphSettings.MaxDurationMargin));

  [TestCase("const:2097152")]
  [TestCase("uniform:1:1e12")]
  [TestCase("lognormal:100:1:max=1e9")]
  public void OutputSizeAboveTheLargestArrayShouldBeRejected(string outputSize)
    => Assert.That(() => Settings(outputSize: outputSize),
                   Throws.TypeOf<FormatException>());

  [TestCase("const:2097151")]
  [TestCase("lognormal:100:1")]
  public void OutputSizeWithinTheLargestArrayShouldBeAccepted(string outputSize)
    => Assert.That(() => Settings(outputSize: outputSize),
                   Throws.Nothing);
}
