﻿// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class InjectionTests
{
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");

  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string> baseConfig = new()
    {
      { "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage" },
      { "Components:QueueStorage", "ArmoniK.Adapters.MongoDB.LockedQueueStorage" },
      { "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage" },
      { "Components:LeaseProvider", "ArmoniK.Adapters.MongoDB.LeaseProvider" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Host)}", "localhost" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Port)}", "3232" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Tls)}", "true" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.User)}", "user" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Password)}", "password" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.CredentialsPath)}", "" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.CAFile)}", "" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ReplicaSet)}", "rs0" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", "database" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DataRetention)}", "10.00:00:00" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelay", "00:00:10" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ObjectStorage)}:ChunkSize", "100000" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.QueueStorage)}:LockRefreshPeriodicity", "00:20:00" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.QueueStorage)}:PollPeriodicity", "00:00:50" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.QueueStorage)}:LockRefreshExtension", "00:50:00" },
    };

    var logger_ = NullLogger.Instance;

    configuration_ = new ConfigurationManager();
    configuration_.AddInMemoryCollection(baseConfig);

    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddSingleton(ActivitySource);
    services.AddLogging();
    provider_ = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });
  }

  private ServiceProvider      provider_;
  private ConfigurationManager configuration_;

  [Test]
  public void MongoDbOptionsNotNull()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();
    Assert.NotNull(options);
  }

  [Test]
  public void MongoDbOptionsValueNotNull()
  {
    var options = configuration_.GetRequiredValue<Options.MongoDB>(Options.MongoDB.SettingSection);
    Assert.NotNull(options);
  }

  [Test]
  public void ReadMongoDbHost()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("localhost",
                    options.Host);
  }

  [Test]
  public void ReadMongoDbUser()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("user",
                    options.User);
  }

  [Test]
  public void ReadMongoDbPassword()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("password",
                    options.Password);
  }

  [Test]
  public void ReadMongoDbCredentialsPath()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("",
                    options.CredentialsPath);
  }

  [Test]
  public void ReadMongoDbCAFile()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("",
                    options.CAFile);
  }

  [Test]
  public void ReadMongoDbPort()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(3232,
                    options.Port);
  }

  [Test]
  public void ReadMongoDbTls()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(true,
                    options.Tls);
  }

  [Test]
  public void ReadMongoDbDatabaseName()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();
    Assert.AreEqual("database",
                    options.DatabaseName);
  }

  [Test]
  public void ReadMongoDbDataRetention()
  {
    var options = provider_.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(TimeSpan.FromDays(10),
                    options.DataRetention);
  }

  [Test]
  public void TableOptionsNotNull()
  {
    var options = provider_.GetRequiredService<TableStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadTablePollingDelay()
  {
    var options = provider_.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(10),
                    options.PollingDelay);
  }

  [Test]
  public void ObjectOptionsNotNull()
  {
    var options = provider_.GetRequiredService<Options.ObjectStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadObjectChunkSize()
  {
    var options = provider_.GetRequiredService<Options.ObjectStorage>();

    Assert.AreEqual(100000,
                    options.ChunkSize);
  }

  [Test]
  public void QueueOptionsNotNull()
  {
    var options = provider_.GetRequiredService<QueueStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadQueueLockRefreshExtension()
  {
    var options = provider_.GetRequiredService<QueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(50),
                    options.LockRefreshExtension);
  }

  [Test]
  public void ReadQueuePollPeriodicity()
  {
    var options = provider_.GetRequiredService<QueueStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(50),
                    options.PollPeriodicity);
  }

  [Test]
  public void ReadQueueLockRefreshPeriodicity()
  {
    var options = provider_.GetRequiredService<QueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(20),
                    options.LockRefreshPeriodicity);
  }

  [Test]
  public void BuildTableStorage()
  {
    var table = provider_.GetRequiredService<TableStorage>();

    Assert.NotNull(table);
  }

  [Test]
  public void TableStorageHasPollingDelay()
  {
    var table = provider_.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(10),
                    table.PollingDelay);
  }

  [Test]
  public void BuildObjectStorageFactory()
  {
    var objectStorageFactory = provider_.GetRequiredService<ObjectStorageFactory>();

    Assert.NotNull(objectStorageFactory);
  }

  [Test]
  public void BuildQueueStorage()
  {
    var queue = provider_.GetRequiredService<LockedQueueStorage>();

    Assert.NotNull(queue);
  }

  [Test]
  public void QueueStorageHasLockRefreshExtension()
  {
    var queue = provider_.GetRequiredService<LockedQueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(50),
                    queue.LockRefreshExtension);
  }

  [Test]
  public void QueueStorageHasPollPeriodicity()
  {
    var queue = provider_.GetRequiredService<LockedQueueStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(50),
                    queue.PollPeriodicity);
  }

  [Test]
  public void QueueStorageHasLockRefreshPeriodicity()
  {
    var queue = provider_.GetRequiredService<LockedQueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(20),
                    queue.LockRefreshPeriodicity);
  }

  [Test]
  public void QueueStorageHasBindingToQueueStorage()
  {
    var queueStorage = provider_.GetRequiredService<ILockedQueueStorage>();

    Assert.NotNull(queueStorage);
    Assert.AreEqual(typeof(LockedQueueStorage),
                    queueStorage.GetType());
  }

  [Test]
  public void ObjectStorageFactoryHasBindingToObjectStorageFactory()
  {
    var objectStorage = provider_.GetRequiredService<IObjectStorageFactory>();

    Assert.NotNull(objectStorage);
    Assert.AreEqual(typeof(ObjectStorageFactory),
                    objectStorage.GetType());
  }
}