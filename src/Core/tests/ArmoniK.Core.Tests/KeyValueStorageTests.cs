using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Tests
{
  [TestFixture("prefix1", TestOf = typeof(KeyValueStorage<,>))]
  [TestFixture("prefix2", TestOf = typeof(KeyValueStorage<,>))]
  [TestFixture("abc", TestOf = typeof(KeyValueStorage<,>))]
  public class KeyValueStorageTests
  {
    private readonly string prefix_;

    public KeyValueStorageTests(string prefix) => prefix_ = prefix;

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public void KeySerializer(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      Assert.AreEqual(taskId, KeyValueStorage<TaskId, Lease>.KeyParser.ParseFrom(taskId.ToByteArray()));
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public void ValueSerializer(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
        { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      Assert.AreEqual(lease, KeyValueStorage<TaskId, Lease>.ValueParser.ParseFrom(lease.ToByteArray()));
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public void KeyCanBeSerializedAndDeserialized(string suffix)
    {
      var objectStorageMock = new Moq.Mock<IObjectStorage>();

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var serializedKey = kvs.SerializeKey(taskId);

      Assert.IsTrue(serializedKey.StartsWith(prefix_));

      var deserializedKey = kvs.DeserializeKey(serializedKey);

      Assert.AreEqual(taskId.Session, deserializedKey.Session);
      Assert.AreEqual(taskId.SubSession, deserializedKey.SubSession);
      Assert.AreEqual(taskId.Task, deserializedKey.Task);

      Console.WriteLine($"{nameof(serializedKey)}={serializedKey}");
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public async Task TryGetValuesForwardsToObjectStorage(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
        { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      var objectStorageMock = new Moq.Mock<IObjectStorage>();

      Expression<Func<IObjectStorage, Task<byte[]>>> expression = storage
        => storage.TryGetValuesAsync(It.IsAny<string>(),
                                     It.IsAny<CancellationToken>());

      objectStorageMock.Setup(expression)
                       .ReturnsAsync(lease.ToByteArray());

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      var obtainedLeaseValue = await kvs.TryGetValuesAsync(taskId, CancellationToken.None);

      objectStorageMock.Verify(expression, Times.Once);
      Assert.AreEqual(lease, obtainedLeaseValue);
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public async Task AddOrUpdateForwardsToObjectStorage(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
        { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      var objectStorageMock = new Moq.Mock<IObjectStorage>();

      Expression<Func<IObjectStorage, Task>> expression = storage
        => storage.AddOrUpdateAsync(It.IsAny<string>(),
                                    It.IsAny<byte[]>(),
                                    It.IsAny<CancellationToken>());

      objectStorageMock.Setup(expression);

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      await kvs.AddOrUpdateAsync(taskId, lease, CancellationToken.None);

      objectStorageMock.Verify(expression, Times.Once);
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public async Task TryDeleteAsyncForwardsToObjectStorage(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
        { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      var objectStorageMock = new Moq.Mock<IObjectStorage>();

      Expression<Func<IObjectStorage, Task<bool>>> expression = storage
        => storage.TryDeleteAsync(It.IsAny<string>(),
                                  It.IsAny<CancellationToken>());

      objectStorageMock.Setup(expression);

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      await kvs.TryDeleteAsync(taskId, CancellationToken.None);

      objectStorageMock.Verify(expression, Times.Once);
    }
  }
}