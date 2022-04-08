using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using Mongo2Go;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class IndexTest
{
  private                 MongoDbRunner   runner_;
  private                 IMongoClient    client_;
  private const           string          DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource  ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");
  private                 ServiceProvider provider_;

  [SetUp]
  public void StartUp()
  {
    var logger = NullLogger.Instance;
    runner_ = MongoDbRunner.Start(singleNodeReplSet: false,
                                  logger: logger);
    client_ = new MongoClient(runner_.ConnectionString);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
    {
      { "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", DatabaseName },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelay", "00:00:10" },
    };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddMongoStorages(configuration,
                              logger);
    services.AddSingleton(ActivitySource);
    services.AddTransient<IMongoClient>(serviceProvider => client_);
    services.AddLogging();

    provider_ = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

  }

  [TearDown]
  public void TearDown()
  {
    client_ = null;
    runner_.Dispose();
  }

  [Test]
  public void IndexCreationShouldSucceed()
  {
    var db         = provider_.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<Dispatch>("Test");
    var taskIndex  = Builders<Dispatch>.IndexKeys.Hashed(model => model.TaskId);

    var indexModels = new CreateIndexModel<Dispatch>[]
    {
      new(taskIndex,
          new()
          {
            Name = nameof(taskIndex),
          }),
    };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List().ToList())
    {
      Console.WriteLine(index);
    }
    Assert.AreEqual(2, collection.Indexes.List().ToList().Count);

  }

  [Test]
  public void IndexCreation2ShouldSucceed()
  {
    var db         = provider_.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<Dispatch>("Test");
    var taskIndex  = Builders<Dispatch>.IndexKeys.Hashed(model => model.TaskId);
    var ttlIndex  = Builders<Dispatch>.IndexKeys.Text(model => model.TimeToLive);

    var indexModels = new CreateIndexModel<Dispatch>[]
    {
      new(taskIndex,
          new()
          {
            Name = nameof(taskIndex),
          }),
      new(ttlIndex,
          new()
          {
            Name = nameof(ttlIndex),
          }),
    };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List().ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(3,
                    collection.Indexes.List().ToList().Count);

  }

  [Test]
  [Ignore("Cannot create combined indexes this way")]
  public void CombinedIndexCreationShouldSucceed()
  {
    var db         = provider_.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<Dispatch>("Test");
    var taskIndex  = Builders<Dispatch>.IndexKeys.Hashed(model => model.TaskId);
    var ttlIndex   = Builders<Dispatch>.IndexKeys.Text(model => model.TimeToLive);
    var combine = Builders<Dispatch>.IndexKeys.Combine(taskIndex,
                                                       ttlIndex);

    var indexModels = new CreateIndexModel<Dispatch>[]
    {
      new(taskIndex,
          new()
          {
            Name = nameof(taskIndex),
          }),
      new(ttlIndex,
          new()
          {
            Name = nameof(ttlIndex),
          }),
      new(combine,
          new()
          {
            Name = nameof(combine),
          }),
    };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List().ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(4,
                    collection.Indexes.List().ToList().Count);

  }

}