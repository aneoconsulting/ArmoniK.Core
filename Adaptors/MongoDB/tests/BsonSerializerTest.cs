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
using System.Globalization;
using System.Linq;
using System.Runtime.Intrinsics.Arm;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf.WellKnownTypes;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using NUnit.Framework;

using Result = ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Result;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class BsonSerializerTest
{

  [Test]
  public void SerializeResultDataModel()
  {
    var rdm = new Result("sessionId",
                         "Key",
                         "Owner",
                         "Origin",
                         true,
                         DateTime.Parse("2022-02-15 8:55:05.954").ToUniversalTime(),
                         new[] { (byte)1, (byte)2, (byte)3 });
    
    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<Result>(serialized);

    Assert.AreEqual(rdm.Id, deserialized.Id);
    Assert.AreEqual(rdm.Key, deserialized.Key);
    Assert.AreEqual(rdm.OwnerTaskId, deserialized.OwnerTaskId);
    Assert.AreEqual(rdm.OriginDispatchId, deserialized.OriginDispatchId);
    Assert.AreEqual(rdm.IsResultAvailable, deserialized.IsResultAvailable);
    Assert.AreEqual(rdm.CreationDate, deserialized.CreationDate);
    Assert.IsTrue(rdm.Data.SequenceEqual(deserialized.Data));

  }


  [Test]
  public void SerializeTaskDataModel()
  {
    var tdm = new TaskData(HasPayload: true,
                           Options: new(Priority: 2,
                                        Options: new Dictionary<string, string>()
                                                 {
                                                   { "key1", "Value1" },
                                                   { "key2", "value2" },
                                                 },
                                        MaxDuration: TimeSpan.FromMinutes(42),
                                        MaxRetries: 7),
                           TaskId: "tid",
                           Payload: new[] { (byte)1, (byte)2, (byte)3 },
                           SessionId: "ses1",
                           Status: TaskStatus.Creating,
                           ParentTaskId: "par",
                           CreationDate: DateTime.Now,
                           DataDependencies: new List<string>
                                             {
                                               "dep1",
                                               "dep2",
                                             },
                           AncestorDispatchIds: new List<string>
                                                {
                                                  "ancestor1",
                                                  "ancestor2",
                                                },
                           DispatchId: "dispatchId1",
                           ExpectedOutput: new List<string>
                                           {
                                             "output1",
                                             "output2",
                                           });

    var serialized = tdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<TaskData>(serialized);

    Assert.IsNotNull(deserialized);
    Assert.AreEqual(tdm.HasPayload,
                    deserialized.HasPayload);
    Assert.IsNotNull(deserialized.Options);
    Assert.AreEqual(tdm.Options.Priority,
                    deserialized.Options.Priority);
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
    Assert.IsTrue(tdm.Payload.SequenceEqual(deserialized.Payload));
    Assert.AreEqual(tdm.Status,
                    deserialized.Status);
    Assert.AreEqual(tdm.ParentTaskId,
                    deserialized.ParentTaskId);
    Assert.AreEqual(tdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(tdm.DispatchId,
                    deserialized.DispatchId);
    Assert.IsTrue(tdm.AncestorDispatchIds.SequenceEqual(deserialized.AncestorDispatchIds));
    Assert.IsTrue(tdm.ExpectedOutput.SequenceEqual(deserialized.ExpectedOutput));
  }
}