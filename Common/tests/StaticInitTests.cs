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
                                                 $"{CertificatePath}:0", Cert1.ToJson()
                                               },
                                               {
                                                 $"{CertificatePath}:1", Cert2.ToJson()
                                               },
                                               {
                                                 $"{UserPath}:0", User1.ToJson()
                                               },
                                               {
                                                 $"{UserPath}:1", User2.ToJson()
                                               },
                                               {
                                                 $"{RolePath}:0", Role1.ToJson()
                                               },
                                               {
                                                 $"{RolePath}:1", Role2.ToJson()
                                               },
                                               {
                                                 $"{PartitionPath}:0", PartitionData1.ToJson()
                                               },
                                               {
                                                 $"{PartitionPath}:1", PartitionData2.ToJson()
                                               },
                                             };

    Environment.SetEnvironmentVariable(CertificatePath.Replace(":",
                                                               "__") + "__2",
                                       Cert3.ToJson());
    Environment.SetEnvironmentVariable(UserPath.Replace(":",
                                                        "__") + "__2",
                                       User3.ToJson());
    Environment.SetEnvironmentVariable(RolePath.Replace(":",
                                                        "__") + "__2",
                                       Role3.ToJson());
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

  private static readonly Certificate Cert1 = new()
                                              {
                                                Fingerprint = "Fingerprint1",
                                                Cn          = "CN1",
                                                User        = "User1",
                                              };

  private static readonly Certificate Cert2 = new()
                                              {
                                                Fingerprint = "Fingerprint2",
                                                Cn          = "CN2",
                                                User        = "User2",
                                              };

  private static readonly Certificate Cert3 = new()
                                              {
                                                Fingerprint = "Fingerprint3",
                                                Cn          = "CN3",
                                                User        = "User3",
                                              };

  private static readonly User User1 = new()
                                       {
                                         Name = "User1",
                                         Roles = new List<string>
                                                 {
                                                   "Role1",
                                                 },
                                       };

  private static readonly User User2 = new()
                                       {
                                         Name = "User2",
                                         Roles = new List<string>
                                                 {
                                                   "Role1",
                                                   "Role2",
                                                 },
                                       };

  private static readonly User User3 = new()
                                       {
                                         Name = "User3",
                                         Roles = new List<string>
                                                 {
                                                   "Role3",
                                                 },
                                       };

  private static readonly Role Role1 = new()
                                       {
                                         Name = "Role1",
                                         Permissions = new List<string>
                                                       {
                                                         "Perm1",
                                                       },
                                       };

  private static readonly Role Role2 = new()
                                       {
                                         Name = "Role2",
                                         Permissions = new List<string>
                                                       {
                                                         "Perm1",
                                                         "Perm2",
                                                       },
                                       };

  private static readonly Role Role3 = new()
                                       {
                                         Name = "Role3",
                                         Permissions = new List<string>
                                                       {
                                                         "Perm3",
                                                       },
                                       };

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

  private const string CertificatePath = $"{InitServices.SettingSection}:{Authentication.SettingSection}:{nameof(Authentication.UserCertificates)}";
  private const string UserPath        = $"{InitServices.SettingSection}:{Authentication.SettingSection}:{nameof(Authentication.Users)}";
  private const string RolePath        = $"{InitServices.SettingSection}:{Authentication.SettingSection}:{nameof(Authentication.Roles)}";
  private const string PartitionPath   = $"{InitServices.SettingSection}:{Partitioning.SettingSection}:{nameof(Partitioning.Partitions)}";

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
    Assert.That(init.Authentication.UserCertificates.Count,
                Is.EqualTo(3));
    Assert.That(init.Authentication.Users.Count,
                Is.EqualTo(3));
    Assert.That(init.Authentication.Roles.Count,
                Is.EqualTo(3));
    Assert.That(init.Partitioning.Partitions.Count,
                Is.EqualTo(3));

    Assert.That(Certificate.FromJson(init.Authentication.UserCertificates[0]),
                Is.EqualTo(Cert1));
    Assert.That(Certificate.FromJson(init.Authentication.UserCertificates[1]),
                Is.EqualTo(Cert2));
    Assert.That(Certificate.FromJson(init.Authentication.UserCertificates[2]),
                Is.EqualTo(Cert3));

    Assert.That(User.FromJson(init.Authentication.Users[0]),
                Is.EqualTo(User1));
    Assert.That(User.FromJson(init.Authentication.Users[1]),
                Is.EqualTo(User2));
    Assert.That(User.FromJson(init.Authentication.Users[2]),
                Is.EqualTo(User3));

    Assert.That(Role.FromJson(init.Authentication.Roles[0]),
                Is.EqualTo(Role1));
    Assert.That(Role.FromJson(init.Authentication.Roles[1]),
                Is.EqualTo(Role2));
    Assert.That(Role.FromJson(init.Authentication.Roles[2]),
                Is.EqualTo(Role3));

    Assert.That(Partition.FromJson(init.Partitioning.Partitions[0]),
                Is.EqualTo(PartitionData1));
    Assert.That(Partition.FromJson(init.Partitioning.Partitions[1]),
                Is.EqualTo(PartitionData2));
    Assert.That(Partition.FromJson(init.Partitioning.Partitions[2]),
                Is.EqualTo(PartitionData3));

    var initDb = provider_!.GetRequiredService<InitDatabase>();
    Assert.That(initDb.Users.Count,
                Is.EqualTo(3));
    Assert.That(initDb.Auths.Count,
                Is.EqualTo(3));
    Assert.That(initDb.Roles.Count,
                Is.EqualTo(3));
    Assert.That(initDb.Partitions.Count,
                Is.EqualTo(3));
  }

  [Test]
  public void NoFingerprintShouldSucceed()
  {
    var cert = Certificate.FromJson("{\"User\": \"User1\", \"Cn\": \"CN1\"}");
    Assert.That(cert.Fingerprint,
                Is.Null);
  }

  [Test]
  public void NullFingerprintShouldSucceed()
  {
    var cert = Certificate.FromJson("{\"User\": \"User1\", \"Cn\": \"CN1\", \"Fingerprint\": null}");
    Assert.That(cert.Fingerprint,
                Is.Null);
  }
}
