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

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(Injection.Options.Pollster))]
public class PollsterOptionsTests
{
  private static Injection.Options.Pollster Bind(int? maxPrefetchAttempts,
                                                 int? nbAcquisitionRetry)
  {
    var config = new Dictionary<string, string?>();

    if (maxPrefetchAttempts is not null)
    {
      config[$"{Injection.Options.Pollster.SettingSection}:MaxPrefetchAttempts"] = maxPrefetchAttempts.ToString();
    }

    if (nbAcquisitionRetry is not null)
    {
      config[$"{Injection.Options.Pollster.SettingSection}:NbAcquisitionRetry"] = nbAcquisitionRetry.ToString();
    }

    return new ConfigurationBuilder().AddInMemoryCollection(config)
                                     .Build()
                                     .GetSection(Injection.Options.Pollster.SettingSection)
                                     .Get<Injection.Options.Pollster>() ?? new Injection.Options.Pollster();
  }

  [Test]
  [TestCase(null,
            null,
            ExpectedResult = 3)]
  [TestCase(0,
            null,
            ExpectedResult = 0)]
  [TestCase(null,
            0,
            ExpectedResult = 0)]
  [TestCase(5,
            0,
            ExpectedResult = 5)]
  [TestCase(0,
            5,
            ExpectedResult = 0)]
  public int MaxPrefetchAttemptsShouldSupportDeprecatedAlias(int? maxPrefetchAttempts,
                                                             int? nbAcquisitionRetry)
    => Bind(maxPrefetchAttempts,
            nbAcquisitionRetry)
      .MaxPrefetchAttempts;
}
