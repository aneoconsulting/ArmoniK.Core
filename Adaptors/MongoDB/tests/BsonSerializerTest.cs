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
using System.Linq;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using Result = ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Result;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class BsonSerializerTest
{
  [Test]
  public void SerializeSessionDataModel()
  {
    var rdm = new SessionData("SessionId",
                              SessionStatus.Running,
                              new [] {"part1", "part2"},
                              new TaskOptions
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                                MaxRetries  = 2,
                                Priority    = 1,
                              });

    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<SessionData>(serialized);

    Assert.AreEqual(rdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(rdm.PartitionIds,
                    deserialized.PartitionIds);
    Assert.IsNotNull(deserialized.Options);
    Assert.AreEqual(rdm.Options.MaxDuration,
                    deserialized.Options.MaxDuration);
    Assert.AreEqual(rdm.Options.MaxRetries,
                    deserialized.Options.MaxRetries);
    Assert.AreEqual(rdm.Options.Priority,
                    deserialized.Options.Priority);
  }

  [Test]
  public void SerializeResultDataModel()
  {
    var rdm = new Result("sessionId",
                         "Key",
                         "OwnerTaskId",
                         ResultStatus.Completed,
                         DateTime.Parse("2022-02-15 8:55:05.954")
                                 .ToUniversalTime(),
                         new[]
                         {
                           (byte)1,
                           (byte)2,
                           (byte)3,
                         });

    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<Result>(serialized);

    Assert.AreEqual(rdm.Id,
                    deserialized.Id);
    Assert.AreEqual(rdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(rdm.Name,
                    deserialized.Name);
    Assert.AreEqual(rdm.OwnerTaskId,
                    deserialized.OwnerTaskId);
    Assert.AreEqual(rdm.Status,
                    deserialized.Status);
    Assert.AreEqual(rdm.CreationDate,
                    deserialized.CreationDate);
    Assert.IsTrue(rdm.Data.SequenceEqual(deserialized.Data));
  }

  [Test]
  public void SerializeTaskDataModel()
  {
    var tdm = new TaskData("SessionId",
                           "TaskCompletedId",
                           "OwnerPodId",
                           "PayloadId",
                           new[]
                           {
                             "parent1",
                           },
                           new[]
                           {
                             "dependency1",
                           },
                           new[]
                           {
                             "output1",
                           },
                           Array.Empty<string>(),
                           TaskStatus.Completed,
                           new Core.Common.Storage.TaskOptions(new Dictionary<string, string>
                                                               {
                                                                 {
                                                                   "key1", "data1"
                                                                 },
                                                                 {
                                                                   "key2", "data2"
                                                                 },
                                                               },
                                                               TimeSpan.FromSeconds(200),
                                                               5,
                                                               1,
                                                               "part1"),
                           new Output(true,
                                      ""));

    var serialized = tdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<TaskData>(serialized);

    Assert.IsNotNull(deserialized);
    Assert.IsNotNull(deserialized.Options);
    Assert.AreEqual(tdm.Options.Priority,
                    deserialized.Options.Priority);
    Assert.AreEqual(tdm.Options.PartitionId,
                    deserialized.Options.PartitionId);
    Assert.IsNotNull(tdm.Options.Options);
    Assert.AreEqual(tdm.Options.Options["key1"],
                    deserialized.Options.Options["key1"]);
    Assert.AreEqual(tdm.Options.Options["key2"],
                    deserialized.Options.Options["key2"]);
    Assert.IsTrue(tdm.DataDependencies.SequenceEqual(deserialized.DataDependencies));
    Assert.AreEqual(tdm.Options.MaxDuration,
                    deserialized.Options.MaxDuration);
    Assert.AreEqual(tdm.Options.MaxRetries,
                    deserialized.Options.MaxRetries);
    Assert.AreEqual(tdm.TaskId,
                    deserialized.TaskId);
    Assert.AreEqual(tdm.Status,
                    deserialized.Status);
    Assert.AreEqual(tdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(tdm.OwnerPodId,
                    deserialized.OwnerPodId);
    Assert.IsTrue(tdm.RetryOfIds.SequenceEqual(deserialized.RetryOfIds));
    Assert.IsTrue(tdm.ExpectedOutputIds.SequenceEqual(deserialized.ExpectedOutputIds));
    Assert.IsTrue(tdm.ParentTaskIds.SequenceEqual(deserialized.ParentTaskIds));
  }

  [Test]
  public void InitializeResultDataModelMapping()
    => _ = new ResultDataModelMapping();

  [Test]
  public void InitializeTaskDataModelMapping()
    => _ = new TaskDataModelMapping();

  [Test]
  public void InitializeSessionDataModelMapping()
    => _ = new SessionDataModelMapping();
}
