// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Collections.Generic;

using ArmoniK.Core.gRPC.V1;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Memory;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Adapters.MongoDB.Tests
{
  [TestFixture]
  [Ignore("Require a deployed database")]
  class TableStorageTests
  {
    private IConfiguration configuration_;

    [SetUp]
    public void SetUp()
    {
      Dictionary<string, string> baseConfig = new()
      {
        { "MongoDB:ConnectionString", "mongodb://localhost" },
        { "MongoDB:DatabaseName", "database" },
        { "MongoDB:DataRetention", "10.00:00:00" },
        { "MongoDB:TableStorage:PollingDelay", "00:00:10" },
        { "MongoDB:LeaseProvider:AcquisitionPeriod", "00:20:00" },
        { "MongoDB:LeaseProvider:AcquisitionDuration", "00:50:00" },
        { "MongoDB:ObjectStorage:ChunkSize", "100000" },
        { "MongoDB:LockedQueueStorage:LockRefreshPeriodicity", "00:20:00" },
        { "MongoDB:LockedQueueStorage:PollPeriodicity", "00:00:50" },
        { "MongoDB:LockedQueueStorage:LockRefreshExtension", "00:50:00" },
      };

      var configSource = new MemoryConfigurationSource
      {
        InitialData = baseConfig,
      };

      var builder = new ConfigurationBuilder()
        .Add(configSource);

      configuration_ = builder.Build();
    }

    [Test]
    public void AddOneTaskAndCount()
    {
      var services = new ServiceCollection();
      services.AddMongoComponents(configuration_);
      services.AddLogging();

      var provider = services.BuildServiceProvider(new ServiceProviderOptions { ValidateOnBuild = true });

      var table = provider.GetRequiredService<TableStorage>();

      Assert.AreEqual(0, table.CountTasksAsync(new TaskFilter()).Result);

      var session = table.CreateSessionAsync(new SessionOptions()
      {
        DefaultTaskOption = new TaskOptions(),
        IdTag             = "tag",
      }).Result;

      var (_, _) = table.InitializeTaskCreation(session, new TaskOptions(), new Payload()).Result;


      Assert.AreEqual(1,
                      table.CountTasksAsync(new TaskFilter()
                      {
                        SessionId    = session.Session,
                        SubSessionId = session.SubSession
                      }).Result);
    }
  }
}