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

using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class IndexTest
{
  [SetUp]
  public void StartUp()
  {
    dbProvider_ = new MongoDatabaseProvider();
    provider_   = dbProvider_.GetServiceProvider();
  }

  [TearDown]
  public void TearDown()
    => dbProvider_?.Dispose();

  private IServiceProvider?      provider_;
  private MongoDatabaseProvider? dbProvider_;

  [Test]
  public void IndexCreationShouldSucceed()
  {
    var db         = provider_!.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<TaskData>("Test");

    var indexModels = new[]
                      {
                        IndexHelper.CreateHashedOrAscendingIndex<TaskData>(model => model.TaskId,
                                                                           true),
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

    var indexModels = new[]
                      {
                        IndexHelper.CreateHashedOrAscendingIndex<TaskData>(model => model.TaskId,
                                                                           true),
                        IndexHelper.CreateAscendingIndex<TaskData>(model => model.PodTtl),
                        IndexHelper.CreateAscendingIndex<TaskData>(model => model.Options.MaxDuration),
                        IndexHelper.CreateAscendingIndex<TaskData>(model => model.CreationDate,
                                                                   expireAfter: TimeSpan.FromDays(1)),
                        IndexHelper.CreateAscendingIndex<TaskData>(model => model.OwnerPodId),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(indexModels.Length + 1,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }

  [Test]
  public void IndexCreationWithMaxExpireShouldSucceed()
  {
    var db         = provider_!.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<TaskData>("Test");

    var indexModels = new[]
                      {
                        IndexHelper.CreateAscendingIndex<TaskData>(model => model.CreationDate,
                                                                   expireAfter: TimeSpan.MaxValue),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(indexModels.Length + 1,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }

  [Test]
  public void IndexCreationWithNullExpireShouldSucceed()
  {
    var db         = provider_!.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<TaskData>("Test");

    var indexModels = new[]
                      {
                        IndexHelper.CreateAscendingIndex<TaskData>(model => model.CreationDate,
                                                                   expireAfter: null),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(indexModels.Length + 1,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }

  [Test]
  public void CombinedIndexCreationShouldSucceed()
  {
    var db         = provider_!.GetRequiredService<IMongoDatabase>();
    var collection = db.GetCollection<TaskData>("Test");

    var indexModels = new[]
                      {
                        IndexHelper.CreateHashedOrAscendingIndex<TaskData>(model => model.TaskId,
                                                                           true),
                        IndexHelper.CreateCombinedIndex<TaskData>(model => model.TaskId,
                                                                  model => model.SessionId),
                        IndexHelper.CreateCombinedIndex<TaskData>(model => model.TaskId,
                                                                  model => model.Status),
                      };

    collection.Indexes.CreateMany(indexModels);
    foreach (var index in collection.Indexes.List()
                                    .ToList())
    {
      Console.WriteLine(index);
    }

    Assert.AreEqual(indexModels.Length + 1,
                    collection.Indexes.List()
                              .ToList()
                              .Count);
  }
}
