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

using ArmoniK.Adapters.MongoDB.Options;
using ArmoniK.Core.Storage;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

using NUnit.Framework;

namespace ArmoniK.Adapters.MongoDB.Tests
{
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
        { "MongoDB:ConnectionString", "mongodb://localhost" },
        { "MongoDB:DatabaseName", "database" },
        { "MongoDB:DataRetention", "10.00:00:00" },
        { "MongoDB:TableStorage:PollingDelay", "00:00:10" },
        { "MongoDB:LeaseProvider:AcquisitionPeriod", "00:20:00" },
        { "MongoDB:LeaseProvider:AcquisitionDuration", "00:50:00" },
        { "MongoDB:ObjectStorage:ChunkSize", "100000" },
        { "MongoDB:QueueStorage:LockRefreshPeriodicity", "00:20:00" },
        { "MongoDB:QueueStorage:PollPeriodicity", "00:00:50" },
        { "MongoDB:QueueStorage:LockRefreshExtension", "00:50:00" },
      };

      var configSource = new MemoryConfigurationSource
                         {
                           InitialData = baseConfig,
                         };

      var builder = new ConfigurationBuilder()
       .Add(configSource);

      configuration_ = builder.Build();
    }

    private IConfiguration configuration_;

    [Test]
    public void MongoDbOptionsNotNull()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.MongoDB>>();

      Assert.NotNull(options.Value);
    }

    [Test]
    public void ReadMongoDbConnectionString()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.MongoDB>>();

      Assert.AreEqual("mongodb://localhost",
                      options.Value.ConnectionString);
    }

    [Test]
    public void ReadMongoDbDatabaseName()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.MongoDB>>();
      Assert.AreEqual("database",
                      options.Value.DatabaseName);
    }

    [Test]
    public void ReadMongoDbDataRetention()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.MongoDB>>();

      Assert.AreEqual(TimeSpan.FromDays(10),
                      options.Value.DataRetention);
    }

    [Test]
    public void TableOptionsNotNull()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.TableStorage>>();

      Assert.NotNull(options.Value);
    }

    [Test]
    public void ReadTablePollingDelay()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.TableStorage>>();

      Assert.AreEqual(TimeSpan.FromSeconds(10),
                      options.Value.PollingDelay);
    }

    [Test]
    public void ObjectOptionsNotNull()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.ObjectStorage>>();

      Assert.NotNull(options.Value);
    }

    [Test]
    public void ReadObjectChunkSize()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.ObjectStorage>>();

      Assert.AreEqual(100000,
                      options.Value.ChunkSize);
    }

    [Test]
    public void QueueOptionsNotNull()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<QueueStorage>>();

      Assert.NotNull(options.Value);
    }

    [Test]
    public void ReadQueueLockRefreshExtension()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<QueueStorage>>();

      Assert.AreEqual(TimeSpan.FromMinutes(50),
                      options.Value.LockRefreshExtension);
    }

    [Test]
    public void ReadQueuePollPeriodicity()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<QueueStorage>>();

      Assert.AreEqual(TimeSpan.FromSeconds(50),
                      options.Value.PollPeriodicity);
    }

    [Test]
    public void ReadQueueLockRefreshPeriodicity()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<QueueStorage>>();

      Assert.AreEqual(TimeSpan.FromMinutes(20),
                      options.Value.LockRefreshPeriodicity);
    }

    [Test]
    public void LeaseOptionsNotNull()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.LeaseProvider>>();

      Assert.NotNull(options.Value);
    }

    [Test]
    public void ReadLeaseAcquisitionPeriod()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.LeaseProvider>>();

      Assert.AreEqual(TimeSpan.FromMinutes(20),
                      options.Value.AcquisitionPeriod);
    }

    [Test]
    public void ReadLeaseAcquisitionDuration()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      var provider = services.BuildServiceProvider();

      var options = provider.GetRequiredService<IOptions<Options.LeaseProvider>>();

      Assert.AreEqual(TimeSpan.FromMinutes(50),
                      options.Value.AcquisitionDuration);
    }

    [Test]
    public void ValidateProvider()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var _ = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });
    }

    [Test]
    public void BuildTableStorage()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var table = provider.GetRequiredService<TableStorage>();

      Assert.NotNull(table);
    }

    [Test]
    public void TableStorageHasPollingDelay()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
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
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var objectStorage = provider.GetRequiredService<ObjectStorage>();

      Assert.NotNull(objectStorage);
    }

    [Test]
    public void BuildQueueStorage()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var queue = provider.GetRequiredService<LockedQueueStorage>();

      Assert.NotNull(queue);
    }

    [Test]
    public void QueueStorageHasLockRefreshExtension()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var queue = provider.GetRequiredService<LockedQueueStorage>();

      Assert.AreEqual(TimeSpan.FromMinutes(50),
                      queue.LockRefreshExtension);
    }

    [Test]
    public void QueueStorageHasPollPeriodicity()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var queue = provider.GetRequiredService<LockedQueueStorage>();

      Assert.AreEqual(TimeSpan.FromSeconds(50),
                      queue.PollPeriodicity);
    }

    [Test]
    public void QueueStorageHasLockRefreshPeriodicity()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var queue = provider.GetRequiredService<LockedQueueStorage>();

      Assert.AreEqual(TimeSpan.FromMinutes(20),
                      queue.LockRefreshPeriodicity);
    }

    [Test]
    public void BuildLeaseProvider()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var table = provider.GetRequiredService<LeaseProvider>();

      Assert.NotNull(table);
    }

    [Test]
    public void LeaseProviderHasAcquisitionPeriod()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider();

      var leaseProvider = provider.GetRequiredService<LeaseProvider>();

      Assert.AreEqual(TimeSpan.FromMinutes(20),
                      leaseProvider.AcquisitionPeriod);
    }

    [Test]
    public void LeaseProviderHasAcquisitionDuration()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider();

      var leaseProvider = provider.GetRequiredService<LeaseProvider>();

      Assert.AreEqual(TimeSpan.FromMinutes(50),
                      leaseProvider.AcquisitionDuration);
    }

    [Test]
    public void TableStorageHasBindingToTableStorage()
    {
      Dictionary<string, string> baseConfig = new()
                                              {
                                                { "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage" },
                                              };
      var configSource = new MemoryConfigurationSource { InitialData = baseConfig };

      var builder = new ConfigurationBuilder().AddConfiguration(configuration_)
                                              .Add(configSource);

      var configuration = builder.Build();

      var services = new ServiceCollection();
      services.AddMongoComponents(configuration);
      services.AddLogging();
      var provider = services.BuildServiceProvider();

      var table = provider.GetRequiredService<ITableStorage>();

      Assert.NotNull(table);
      Assert.AreEqual(typeof(TableStorage),
                      table.GetType());
    }

    [Test]
    public void QueueStorageHasBindingToQueueStorage()
    {
      Dictionary<string, string> baseConfig = new()
                                              {
                                                { "Components:LockedQueueStorage", "ArmoniK.Adapters.MongoDB.LockedQueueStorage" },
                                              };
      var configSource = new MemoryConfigurationSource { InitialData = baseConfig };

      var builder = new ConfigurationBuilder().AddConfiguration(configuration_)
                                              .Add(configSource);

      var configuration = builder.Build();

      var services = new ServiceCollection();
      services.AddMongoComponents(configuration);
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
      var configSource = new MemoryConfigurationSource { InitialData = baseConfig };

      var builder = new ConfigurationBuilder().AddConfiguration(configuration_)
                                              .Add(configSource);

      var configuration = builder.Build();

      var services = new ServiceCollection();
      services.AddMongoComponents(configuration);
      services.AddLogging();
      var provider = services.BuildServiceProvider();

      var objectStorage = provider.GetRequiredService<IObjectStorage>();

      Assert.NotNull(objectStorage);
      Assert.AreEqual(typeof(ObjectStorage),
                      objectStorage.GetType());
    }

    [Test]
    public void LeaseProviderHasBindingToLeaseProvider()
    {
      Dictionary<string, string> baseConfig = new()
                                              {
                                                { "Components:LeaseProvider", "ArmoniK.Adapters.MongoDB.LeaseProvider" },
                                              };
      var configSource = new MemoryConfigurationSource { InitialData = baseConfig };

      var builder = new ConfigurationBuilder().AddConfiguration(configuration_)
                                              .Add(configSource);

      var configuration = builder.Build();

      var services = new ServiceCollection();
      services.AddMongoComponents(configuration);
      services.AddLogging();
      var provider = services.BuildServiceProvider();

      var leaseProvider = provider.GetRequiredService<ILeaseProvider>();

      Assert.NotNull(leaseProvider);
      Assert.AreEqual(typeof(LeaseProvider),
                      leaseProvider.GetType());
    }
  }
}
