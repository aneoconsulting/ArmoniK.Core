using System;
using System.Collections.Generic;
using System.Diagnostics;

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
    services.AddTransient(_ => client_);
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
    runner_?.Dispose();
  }

  private                 MongoDbRunner?   runner_;
  private                 IMongoClient?    client_;
  private const           string           DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource   ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");
  private                 ServiceProvider? provider_;

  [Test]
  public void IndexCreationShouldSucceed()
  {
    var db         = provider_!.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<TaskData>("Test");
    var taskIndex  = Builders<TaskData>.IndexKeys.Hashed(model => model.TaskId);

    var indexModels = new CreateIndexModel<TaskData>[]
                      {
                        new(taskIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(taskIndex),
                            }),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(2,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }

  [Test]
  public void IndexCreation2ShouldSucceed()
  {
    var db         = provider_!.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<TaskData>("Test");
    var taskIndex  = Builders<TaskData>.IndexKeys.Hashed(model => model.TaskId);
    var ttlIndex   = Builders<TaskData>.IndexKeys.Text(model => model.PodTtl);

    var indexModels = new CreateIndexModel<TaskData>[]
                      {
                        new(taskIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(taskIndex),
                            }),
                        new(ttlIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(ttlIndex),
                            }),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(3,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }

  [Test]
  [Ignore("Cannot create combined indexes this way")]
  public void CombinedIndexCreationShouldSucceed()
  {
    var db           = provider_!.GetRequiredService<IMongoDatabase>();
    var collection   = db.GetCollection<TaskData>("Test");
    var taskIndex    = Builders<TaskData>.IndexKeys.Hashed(model => model.TaskId);
    var sessionIndex = Builders<TaskData>.IndexKeys.Text(model => model.SessionId);
    var combine = Builders<TaskData>.IndexKeys.Combine(taskIndex,
                                                       sessionIndex);

    var indexModels = new CreateIndexModel<TaskData>[]
                      {
                        new(taskIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(taskIndex),
                            }),
                        new(sessionIndex,
                            new CreateIndexOptions
                            {
                              Name = nameof(sessionIndex),
                            }),
                        new(combine,
                            new CreateIndexOptions
                            {
                              Name = nameof(combine),
                            }),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(4,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }
}
