// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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

using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class InjectionTests
{
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
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.CAFile)}", "/credentials/ca" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ReplicaSet)}", "rs0" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", "database" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DataRetention)}", "10.00:00:00" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelay", "00:00:10" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ObjectStorage)}:ChunkSize", "100000" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.QueueStorage)}:LockRefreshPeriodicity", "00:20:00" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.QueueStorage)}:PollPeriodicity", "00:00:50" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.QueueStorage)}:LockRefreshExtension", "00:50:00" },
    };

    configuration_ = new ConfigurationManager();
    configuration_.AddInMemoryCollection(baseConfig);

    logger_ = NullLogger.Instance;
  }

  private ConfigurationManager configuration_;
  private ILogger              logger_;

  [Test]
  public void MongoDbOptionsNotNull()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadMongoDbHost()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("localhost",
                    options.Host);
  }

  [Test]
  public void ReadMongoDbUser()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("user",
                    options.User);
  }

  [Test]
  public void ReadMongoDbPassword()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("password",
                    options.Password);
  }

  [Test]
  public void ReadMongoDbCredentialsPath()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("",
                    options.CredentialsPath);
  }

  [Test]
  public void ReadMongoDbCAFile()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("/credentials/ca",
                    options.CAFile);
  }

  [Test]
  public void ReadMongoDbPort()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(3232,
                    options.Port);
  }

  [Test]
  public void ReadMongoDbTls()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(true,
                    options.Tls);
  }

  [Test]
  public void ReadMongoDbDatabaseName()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();
    Assert.AreEqual("database",
                    options.DatabaseName);
  }

  [Test]
  public void ReadMongoDbDataRetention()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(TimeSpan.FromDays(10),
                    options.DataRetention);
  }

  [Test]
  public void TableOptionsNotNull()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.TableStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadTablePollingDelay()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(10),
                    options.PollingDelay);
  }

  [Test]
  public void ObjectOptionsNotNull()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.ObjectStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadObjectChunkSize()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.ObjectStorage>();

    Assert.AreEqual(100000,
                    options.ChunkSize);
  }

  [Test]
  public void QueueOptionsNotNull()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.QueueStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadQueueLockRefreshExtension()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<Options.QueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(50),
                    options.LockRefreshExtension);
  }

  [Test]
  public void ReadQueuePollPeriodicity()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<QueueStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(50),
                    options.PollPeriodicity);
  }

  [Test]
  public void ReadQueueLockRefreshPeriodicity()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    var provider = services.BuildServiceProvider();

    var options = provider.GetRequiredService<QueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(20),
                    options.LockRefreshPeriodicity);
  }

  [Test]
  public void ValidateProvider()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var _ = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });
  }

  [Test]
  public void BuildTableStorage()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    var table = provider.GetRequiredService<TableStorage>();

    Assert.NotNull(table);
  }

  [Test]
  public void TableStorageHasPollingDelay()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider();

    var table = provider.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(10),
                    table.PollingDelay);
  }

  [Test]
  public void BuildObjectStorage()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    var objectStorage = provider.GetRequiredService<ObjectStorage>();

    Assert.NotNull(objectStorage);
  }

  [Test]
  public void BuildQueueStorage()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    var queue = provider.GetRequiredService<LockedQueueStorage>();

    Assert.NotNull(queue);
  }

  [Test]
  public void QueueStorageHasLockRefreshExtension()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    var queue = provider.GetRequiredService<LockedQueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(50),
                    queue.LockRefreshExtension);
  }

  [Test]
  public void QueueStorageHasPollPeriodicity()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    var queue = provider.GetRequiredService<LockedQueueStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(50),
                    queue.PollPeriodicity);
  }

  [Test]
  public void QueueStorageHasLockRefreshPeriodicity()
  {
    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    var queue = provider.GetRequiredService<LockedQueueStorage>();

    Assert.AreEqual(TimeSpan.FromMinutes(20),
                    queue.LockRefreshPeriodicity);
  }

  [Test]
  public void QueueStorageHasBindingToQueueStorage()
  {
    Dictionary<string, string> baseConfig = new()
    {
      { "Components:LockedQueueStorage", "ArmoniK.Adapters.MongoDB.LockedQueueStorage" },
    };

    configuration_.AddInMemoryCollection(baseConfig);

    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();
    var provider = services.BuildServiceProvider();

    var queueStorage = provider.GetRequiredService<ILockedQueueStorage>();

    Assert.NotNull(queueStorage);
    Assert.AreEqual(typeof(LockedQueueStorage),
                    queueStorage.GetType());
  }

  [Test]
  public void ObjectStorageHasBindingToObjectStorage()
  {
    Dictionary<string, string> baseConfig = new()
    {
      { "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage" },
    };
    configuration_.AddInMemoryCollection(baseConfig);

    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger_);
    services.AddLogging();
    var provider = services.BuildServiceProvider();

    var objectStorage = provider.GetRequiredService<IObjectStorage>();

    Assert.NotNull(objectStorage);
    Assert.AreEqual(typeof(ObjectStorage),
                    objectStorage.GetType());
  }
}