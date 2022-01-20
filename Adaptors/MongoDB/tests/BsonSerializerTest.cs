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
using System.Linq;

using ArmoniK.Core.Adapters.MongoDB.Table;

using Google.Protobuf.WellKnownTypes;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class BsonSerializerTest
{
  [Test]
  public void SerializeTaskDataModel()
  {
    var tdm = new TaskDataModel
              {
                HasPayload = true,
                Options = new()
                          {
                            Priority = 2,
                            Options =
                            {
                              { "key1", "Value1" },
                              { "key2", "value2" },
                            },
                            IdTag       = "tag",
                            MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(42)),
                            MaxRetries  = 7,
                          },
                TaskId       = "tid",
                Payload      = new[] { (byte)1, (byte)2, (byte)3 },
                Retries      = 3,
                SessionId    = "ses1",
                Status       = TaskStatus.Creating,
                SubSessionId = "sub1",
                Dependencies = new[] { "dep1", "dep2" },
              };

    var serialized = tdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<TaskDataModel>(serialized);

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
    Assert.IsTrue(tdm.Dependencies.SequenceEqual(deserialized.Dependencies));
    Assert.AreEqual(tdm.Options.IdTag,
                    deserialized.Options.IdTag);
    Assert.AreEqual(tdm.Options.MaxDuration,
                    deserialized.Options.MaxDuration);
    Assert.AreEqual(tdm.Options.MaxRetries,
                    deserialized.Options.MaxRetries);
    Assert.AreEqual(tdm.TaskId,
                    deserialized.TaskId);
    Assert.IsTrue(tdm.Payload.SequenceEqual(deserialized.Payload));
    Assert.AreEqual(tdm.Retries,
                    deserialized.Retries);
    Assert.AreEqual(tdm.Status,
                    deserialized.Status);
    Assert.AreEqual(tdm.SubSessionId,
                    deserialized.SubSessionId);
    Assert.AreEqual(tdm.SessionId,
                    deserialized.SessionId);
  }
}