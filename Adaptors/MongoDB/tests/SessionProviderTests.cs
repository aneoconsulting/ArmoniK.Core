// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Common;

using EphemeralMongo;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
public class SessionProviderTests
{
  [TearDown]
  public void TearDown()
  {
    client_ = null;
    runner_?.Dispose();
    provider_?.Dispose();
    provider_ = null;
  }

  [SetUp]
  public void SetUp()
  {
    var logger = NullLogger.Instance;
    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = false,
                    StandardOuputLogger     = line => logger.LogInformation(line),
                    StandardErrorLogger     = line => logger.LogError(line),
                  };

    runner_ = MongoRunner.Run(options);
    client_ = new MongoClient(runner_.ConnectionString);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
                                               {
                                                 {
                                                   "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage"
                                                 },
                                                 {
                                                   $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", DatabaseName
                                                 },
                                                 {
                                                   $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelay", "00:00:10"
                                                 },
                                               };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddMongoStorages(configuration,
                              logger);
    services.AddSingleton(ActivitySource);
    services.AddTransient<IMongoClient>(_ => client_);
    services.AddLogging();

    provider_ = services.BuildServiceProvider(new ServiceProviderOptions
                                              {
                                                ValidateOnBuild = true,
                                              });
  }

  private                 MongoClient?     client_;
  private                 IMongoRunner?    runner_;
  private const           string           DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource   ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");
  private                 ServiceProvider? provider_;


  [Test]
  public async Task SessionProviderInitShouldSucceed()
  {
    var sessionProvider = provider_!.GetRequiredService<SessionProvider>();

    Assert.NotNull(sessionProvider);

    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await sessionProvider.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await sessionProvider.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false)).Status);
    Assert.AreNotEqual(HealthStatus.Healthy,
                       (await sessionProvider!.Check(HealthCheckTag.Liveness)
                                              .ConfigureAwait(false)).Status);

    await sessionProvider.Init(CancellationToken.None)
                         .ConfigureAwait(false);

    Assert.AreEqual(HealthStatus.Healthy,
                    (await sessionProvider.Check(HealthCheckTag.Liveness)
                                          .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await sessionProvider.Check(HealthCheckTag.Readiness)
                                          .ConfigureAwait(false)).Status);
    Assert.AreEqual(HealthStatus.Healthy,
                    (await sessionProvider.Check(HealthCheckTag.Startup)
                                          .ConfigureAwait(false)).Status);

    Assert.NotNull(sessionProvider.Get());
  }

  [Test]
  public void SessionProviderGetBeforeInitShouldThrow()
  {
    var sessionProvider = provider_!.GetRequiredService<SessionProvider>();

    Assert.NotNull(sessionProvider);

    Assert.Throws<NullReferenceException>(() => sessionProvider.Get());
  }
}
