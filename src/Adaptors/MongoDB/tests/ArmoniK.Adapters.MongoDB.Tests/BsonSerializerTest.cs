using System;
using System.Linq;

using ArmoniK.Core.gRPC.V1;

using Google.Protobuf.WellKnownTypes;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using NUnit.Framework;

using TaskStatus = ArmoniK.Core.gRPC.V1.TaskStatus;

namespace ArmoniK.Adapters.MongoDB.Tests
{
  [TestFixture]
  internal class BsonSerializerTest
  {

    [Test]
    public void SerializeTaskDataModel()
    {
      var tdm = new TaskDataModel
                {
                  HasPayload = true,
                  Options = new TaskOptions
                            {
                              Priority = 2,
                              Options =
                              {
                                { "key1", "Value1" },
                                { "key2", "value2" },
                              },
                              Dependencies =
                              {
                                new TaskId
                                {
                                  SubSession = "sub1",
                                  Session    = "ses1",
                                  Task       = "dep1",
                                },
                                new TaskId
                                {
                                  SubSession = "sub1",
                                  Session    = "ses1",
                                  Task       = "dep2",
                                }
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
                };

      var serialized = tdm.ToBson();

      var deserialized = BsonSerializer.Deserialize<TaskDataModel>(serialized);

      Assert.IsNotNull(deserialized);
      Assert.AreEqual(tdm.HasPayload, deserialized.HasPayload);
      Assert.IsNotNull(deserialized.Options);
      Assert.AreEqual(tdm.Options.Priority, deserialized.Options.Priority);
      Assert.IsNotNull(tdm.Options.Options);
      Assert.AreEqual(tdm.Options.Options["key1"], deserialized.Options.Options["key1"]);
      Assert.AreEqual(tdm.Options.Options["key2"], deserialized.Options.Options["key2"]);
      Assert.IsTrue(tdm.Options.Dependencies.SequenceEqual(deserialized.Options.Dependencies));
      Assert.AreEqual(tdm.Options.IdTag, deserialized.Options.IdTag);
      Assert.AreEqual(tdm.Options.MaxDuration, deserialized.Options.MaxDuration);
      Assert.AreEqual(tdm.Options.MaxRetries, deserialized.Options.MaxRetries);
      Assert.AreEqual(tdm.TaskId, deserialized.TaskId);
      Assert.IsTrue(tdm.Payload.SequenceEqual(deserialized.Payload));
      Assert.AreEqual(tdm.Retries, deserialized.Retries);
      Assert.AreEqual(tdm.Status, deserialized.Status);
      Assert.AreEqual(tdm.SubSessionId, deserialized.SubSessionId);
      Assert.AreEqual(tdm.SessionId, deserialized.SessionId);


    }

  }
}
