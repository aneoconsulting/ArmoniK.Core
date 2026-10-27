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

using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Tests.Helpers;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture(TestOf = typeof(TaskProcessingRegistry))]
public class TaskProcessingRegistryTest
{
  [OneTimeSetUp]
  public void BeforeAll()
    => provider_ = new TestTaskHandlerProvider();

  [OneTimeTearDown]
  public void AfterAll()
  {
    provider_?.Dispose();
    provider_ = null;
  }

  private TestTaskHandlerProvider? provider_;

  private static SimpleQueueMessageHandler Message(string taskId,
                                                   string messageId)
    => new()
       {
         TaskId    = taskId,
         MessageId = messageId,
       };

  private TaskHandler Handler(SimpleQueueMessageHandler message)
    => provider_!.CreateTaskHandler(message);

  [Test]
  public void RemoveShouldForgetTask()
  {
    var registry = new TaskProcessingRegistry();
    var message  = Message("task",
                           "message");
    var handler = Handler(message);

    Assert.That(registry.TryAdd(message,
                                handler),
                Is.True);
    Assert.That(registry.IsEmpty,
                Is.False);

    registry.Remove(message,
                    handler);

    Assert.That(registry.IsEmpty,
                Is.True);
    Assert.That(registry.Snapshot(),
                Is.Empty);
    Assert.That(registry.Handlers(),
                Is.Empty);
  }

  [Test]
  public void RemovingOneMessageShouldKeepTheOthersOfTheTask()
  {
    var registry = new TaskProcessingRegistry();
    var message1 = Message("task",
                           "message1");
    var message2 = Message("task",
                           "message2");
    var first  = Handler(message1);
    var second = Handler(message2);

    Assert.That(registry.TryAdd(message1,
                                first),
                Is.True);
    Assert.That(registry.TryAdd(message2,
                                second),
                Is.True);

    registry.Remove(message2,
                    second);

    Assert.That(registry.Snapshot(),
                Does.ContainKey("task")
                    .WithValue(new[]
                               {
                                 "message1",
                               }));
    Assert.That(registry.Handlers(),
                Is.EqualTo(new[]
                           {
                             first,
                           }));
  }

  [Test]
  public void RejectedHandlerShouldNotRemoveTrackedHandler()
  {
    var registry  = new TaskProcessingRegistry();
    var message   = Message("task",
                            "message");
    var tracked   = Handler(message);
    var duplicate = Handler(message);

    Assert.That(registry.TryAdd(message,
                                tracked),
                Is.True);
    Assert.That(registry.TryAdd(message,
                                duplicate),
                Is.False);

    registry.Remove(message,
                    duplicate);

    Assert.That(registry.Handlers(),
                Is.EqualTo(new[]
                           {
                             tracked,
                           }));
  }

  [Test]
  public void RemoveOfUnknownEntryShouldDoNothing()
  {
    var registry = new TaskProcessingRegistry();
    var message  = Message("task",
                           "message");
    var handler = Handler(message);

    Assert.That(registry.TryAdd(message,
                                handler),
                Is.True);

    registry.Remove(Message("other",
                            "message"),
                    handler);
    registry.Remove(Message("task",
                            "other"),
                    handler);

    Assert.That(registry.Handlers(),
                Is.EqualTo(new[]
                           {
                             handler,
                           }));
  }

  [Test]
  public void SnapshotShouldBeACopy()
  {
    var registry = new TaskProcessingRegistry();
    var message  = Message("task",
                           "message");
    var handler = Handler(message);

    registry.TryAdd(message,
                    handler);

    var snapshot = registry.Snapshot();
    var handlers = registry.Handlers();

    registry.Remove(message,
                    handler);

    Assert.That(snapshot,
                Does.ContainKey("task"));
    Assert.That(handlers,
                Is.EqualTo(new[]
                           {
                             handler,
                           }));
  }
}
