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

using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
internal class StaticInitTests
{
  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string?> baseConfig = new()
                                             {
                                               {
                                                 $"{PartitionPath}:0", PartitionData1.ToJson()
                                               },
                                               {
                                                 $"{PartitionPath}:1", PartitionData2.ToJson()
                                               },
                                             };

    Environment.SetEnvironmentVariable(PartitionPath.Replace(":",
                                                             "__") + "__2",
                                       PartitionData3.ToJson());

    configuration_ = new ConfigurationManager();
    configuration_.AddInMemoryCollection(baseConfig);
    configuration_.AddEnvironmentVariables();

    var services = new ServiceCollection();
    services.AddInitializedOption<InitServices>(configuration_,
                                                InitServices.SettingSection);
    services.AddSingleton<InitDatabase>();
    provider_ = services.BuildServiceProvider();

    logger_ = new LoggerInit(configuration_).GetLogger();
  }

  private static readonly Partition PartitionData1 = new()
                                                     {
                                                       Priority             = 2,
                                                       PodMax               = 2,
                                                       PodReserved          = 2,
                                                       PreemptionPercentage = 2,
                                                       PartitionId          = "Partition1",
                                                       ParentPartitionIds = new List<string>
                                                                            {
                                                                              "PartitionParent1",
                                                                            },
                                                       PodConfiguration = new Dictionary<string, string>
                                                                          {
                                                                            {
                                                                              "key1", "val1"
                                                                            },
                                                                            {
                                                                              "key2", "val2"
                                                                            },
                                                                          },
                                                     };

  private static readonly Partition PartitionData2 = new()
                                                     {
                                                       Priority             = 2,
                                                       PodMax               = 2,
                                                       PodReserved          = 2,
                                                       PreemptionPercentage = 2,
                                                       PartitionId          = "Partition2",
                                                       ParentPartitionIds = new List<string>
                                                                            {
                                                                              "PartitionParent1",
                                                                              "PartitionParent2",
                                                                            },
                                                       PodConfiguration = new Dictionary<string, string>
                                                                          {
                                                                            {
                                                                              "key1", "val1"
                                                                            },
                                                                            {
                                                                              "key3", "val3"
                                                                            },
                                                                          },
                                                     };

  private static readonly Partition PartitionData3 = new()
                                                     {
                                                       Priority             = 2,
                                                       PodMax               = 2,
                                                       PodReserved          = 2,
                                                       PreemptionPercentage = 2,
                                                       PartitionId          = "Partition3",
                                                       ParentPartitionIds = new List<string>
                                                                            {
                                                                              "PartitionParent1",
                                                                              "PartitionParent3",
                                                                            },
                                                     };

  private const string PartitionPath = $"{InitServices.SettingSection}:{Partitioning.SettingSection}:{nameof(Partitioning.Partitions)}";

  private ServiceProvider?      provider_;
  private ConfigurationManager? configuration_;
  private ILogger?              logger_;

  [Test]
  public void InitServicesNotNull()
  {
    var init = provider_!.GetRequiredService<InitServices>();
    Assert.That(init,
                Is.Not.Null);
    Assert.That(init.InitDatabase,
                Is.True);
    Assert.That(init.InitObjectStorage,
                Is.True);
    Assert.That(init.InitQueue,
                Is.True);
  }

  [Test]
  public void CountShouldBePositive()
  {
    var init = provider_!.GetRequiredService<InitServices>();
    logger_!.LogInformation("{@init}",
                            init);

    Assert.That(init.Partitioning.Partitions.Select(Partition.FromJson),
                Is.EqualTo(new List<Partition>
                           {
                             PartitionData1,
                             PartitionData2,
                             PartitionData3,
                           }));

    var initDb = provider_!.GetRequiredService<InitDatabase>();
    Assert.That(initDb.Partitions.Count,
                Is.EqualTo(3));
  }

  [Test]
  public void NullPodConfigurationShouldSucceed()
  {
    var cert =
      Partition.FromJson("{\"ParentPartitionIds\":[],\"PartitionId\":\"TestPartition0\",\"PodConfiguration\":null,\"PodMax\":100,\"PodReserved\":50,\"PreemptionPercentage\":20,\"Priority\":1}");
    Assert.That(cert.PodConfiguration,
                Is.Null);
  }

  [Test]
  public void NoPodConfigurationShouldSucceed()
  {
    var cert =
      Partition.FromJson("{\"ParentPartitionIds\":[],\"PartitionId\":\"TestPartition0\",\"PodMax\":100,\"PodReserved\":50,\"PreemptionPercentage\":20,\"Priority\":1}");
    Assert.That(cert.PodConfiguration,
                Is.Empty);
  }
}
