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
  [TestFixture(TestOf = typeof(KeyValueStorage<,>))]
  public class KeyValueStorageTests
  {
    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public void KeySerializer(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      Assert.AreEqual(taskId,
                      KeyValueStorage<TaskId, Lease>.KeyParser.ParseFrom(taskId.ToByteArray()));
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public void ValueSerializer(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
                  { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      Assert.AreEqual(lease,
                      KeyValueStorage<TaskId, Lease>.ValueParser.ParseFrom(lease.ToByteArray()));
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public void KeyCanBeSerializedAndDeserialized(string suffix)
    {
      var objectStorageMock = new Mock<IObjectStorage>();

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var serializedKey = kvs.SerializeKey(taskId);

      var deserializedKey = kvs.DeserializeKey(serializedKey);

      Assert.AreEqual(taskId.Session,
                      deserializedKey.Session);
      Assert.AreEqual(taskId.SubSession,
                      deserializedKey.SubSession);
      Assert.AreEqual(taskId.Task,
                      deserializedKey.Task);

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

      var objectStorageMock = new Mock<IObjectStorage>();

      Expression<Func<IObjectStorage, Task<byte[]>>> expression = storage
                                                                    => storage.TryGetValuesAsync(It.IsAny<string>(),
                                                                                                 It.IsAny<CancellationToken>());

      objectStorageMock.Setup(expression)
                       .ReturnsAsync(lease.ToByteArray());

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      var obtainedLeaseValue = await kvs.TryGetValuesAsync(taskId,
                                                           CancellationToken.None);

      objectStorageMock.Verify(expression,
                               Times.Once);
      Assert.AreEqual(lease,
                      obtainedLeaseValue);
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public async Task AddOrUpdateForwardsToObjectStorage(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
                  { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      var objectStorageMock = new Mock<IObjectStorage>();

      Expression<Func<IObjectStorage, Task>> expression = storage
                                                            => storage.AddOrUpdateAsync(It.IsAny<string>(),
                                                                                        It.IsAny<byte[]>(),
                                                                                        It.IsAny<CancellationToken>());

      objectStorageMock.Setup(expression);

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      await kvs.AddOrUpdateAsync(taskId,
                                 lease,
                                 CancellationToken.None);

      objectStorageMock.Verify(expression,
                               Times.Once);
    }

    [TestCase("suffix1")]
    [TestCase("suffix2")]
    [TestCase("abc")]
    public async Task TryDeleteAsyncForwardsToObjectStorage(string suffix)
    {
      var taskId = new TaskId { Session = $"session{suffix}", SubSession = $"subSession{suffix}", Task = $"Task{suffix}" };

      var lease = new Lease
                  { Id = taskId, ExpirationDate = Timestamp.FromDateTime(DateTime.UtcNow), LeaseId = $"leaseId{suffix}" };

      var objectStorageMock = new Mock<IObjectStorage>();

      Expression<Func<IObjectStorage, Task<bool>>> expression = storage
                                                                  => storage.TryDeleteAsync(It.IsAny<string>(),
                                                                                            It.IsAny<CancellationToken>());

      objectStorageMock.Setup(expression);

      var kvs = new KeyValueStorage<TaskId, Lease>(objectStorageMock.Object);

      await kvs.TryDeleteAsync(taskId,
                               CancellationToken.None);

      objectStorageMock.Verify(expression,
                               Times.Once);
    }
  }
}
