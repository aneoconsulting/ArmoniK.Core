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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture(TestOf = typeof(QueueMessageReleaser))]
public class QueueMessageReleaserTest
{
  [SetUp]
  public void SetUp()
  {
    lifetime_ = new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance);
    provider_ = new ServiceCollection().AddMetrics()
                                       .AddSingleton<AgentIdentifier>()
                                       .AddSingleton<MeterHolder>()
                                       .BuildServiceProvider();
    exceptionManager_ = new ExceptionManager(lifetime_,
                                             new OptionsWrapper<ConsoleLifetimeOptions>(new ConsoleLifetimeOptions()),
                                             new HostingEnvironment(),
                                             new OptionsWrapper<HostOptions>(new HostOptions()),
                                             NullLogger<ExceptionManager>.Instance,
                                             new ExceptionManager.Options(TimeSpan.Zero,
                                                                          TimeSpan.Zero,
                                                                          0));
  }

  [TearDown]
  public void TearDown()
  {
    lifetime_.StopApplication();
    exceptionManager_.Dispose();
    provider_.Dispose();
  }

  private ApplicationLifetime lifetime_         = null!;
  private ExceptionManager    exceptionManager_ = null!;
  private ServiceProvider     provider_         = null!;

  private QueueMessageReleaser CreateReleaser(int capacity    = 100,
                                              int parallelism = 8)
    => new(new Injection.Options.Pollster
           {
             MessageReleaseQueueCapacity = capacity,
             MessageReleaseParallelism   = parallelism,
           },
           exceptionManager_,
           provider_.GetRequiredService<MeterHolder>(),
           NullLogger<QueueMessageReleaser>.Instance);

  [Test]
  public void InvalidOptionsShouldThrow([Values(0,
                                                -1)]
                                        int value)
  {
    Assert.That(() => CreateReleaser(value),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
    Assert.That(() => CreateReleaser(parallelism: value),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
  }

  [Test]
  [Timeout(10000)]
  public async Task DisposeShouldNotWaitForTheRelease()
  {
    using var releaser = CreateReleaser();
    await releaser.StartAsync(CancellationToken.None)
                  .ConfigureAwait(false);

    var inner = new BlockingQueueMessageHandler();
    var wrapper = new AsyncReleaseQueueMessageHandler(inner,
                                                      releaser);

    // The release of the inner message is blocked, the disposal of the wrapper must return anyway
    await wrapper.DisposeAsync()
                 .AsTask()
                 .WaitAsync(TimeSpan.FromSeconds(5))
                 .ConfigureAwait(false);

    await inner.Started.WaitAsync(TimeSpan.FromSeconds(5))
               .ConfigureAwait(false);
    Assert.That(inner.Completed.IsCompleted,
                Is.False);

    inner.Unblock();
    await inner.Completed.WaitAsync(TimeSpan.FromSeconds(5))
               .ConfigureAwait(false);
  }

  [Test]
  [Timeout(10000)]
  public async Task DoubleDisposeShouldReleaseOnce()
  {
    using var releaser = CreateReleaser();
    await releaser.StartAsync(CancellationToken.None)
                  .ConfigureAwait(false);

    var inner = new CountingQueueMessageHandler();
    var wrapper = new AsyncReleaseQueueMessageHandler(inner,
                                                      releaser);

    await wrapper.DisposeAsync()
                 .ConfigureAwait(false);
    await wrapper.DisposeAsync()
                 .ConfigureAwait(false);

    await inner.Disposed.WaitAsync(TimeSpan.FromSeconds(5))
               .ConfigureAwait(false);
    // Leave some time to a hypothetical second release
    await Task.Delay(100)
              .ConfigureAwait(false);

    Assert.That(inner.DisposeCount,
                Is.EqualTo(1));
  }

  [Test]
  public void MembersShouldBeForwarded()
  {
    using var releaser = CreateReleaser();
    var inner = new SimpleQueueMessageHandler
                {
                  MessageId = "msg",
                  TaskId    = "task",
                  ReceptionDateTime = new DateTime(2026,
                                                   1,
                                                   1,
                                                   0,
                                                   0,
                                                   0,
                                                   DateTimeKind.Utc),
                };
    var wrapper = new AsyncReleaseQueueMessageHandler(inner,
                                                      releaser)
                  {
                    Status = QueueMessageStatus.Postponed,
                  };

    Assert.Multiple(() =>
                    {
                      Assert.That(wrapper.MessageId,
                                  Is.EqualTo("msg"));
                      Assert.That(wrapper.TaskId,
                                  Is.EqualTo("task"));
                      Assert.That(wrapper.ReceptionDateTime,
                                  Is.EqualTo(inner.ReceptionDateTime));
                      // Status is read by the adapter upon release: it must be set on the inner message
                      Assert.That(inner.Status,
                                  Is.EqualTo(QueueMessageStatus.Postponed));
                    });
  }

  [Test]
  [Timeout(10000)]
  public async Task FullQueueShouldReleaseInline()
  {
    // The releaser is not started: nothing is read from its queue
    using var releaser = CreateReleaser(1);

    var first  = new SimpleQueueMessageHandler();
    var second = new SimpleQueueMessageHandler();

    await new AsyncReleaseQueueMessageHandler(first,
                                              releaser).DisposeAsync()
                                                       .ConfigureAwait(false);
    await new AsyncReleaseQueueMessageHandler(second,
                                              releaser).DisposeAsync()
                                                       .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      // First one is waiting in the queue
                      Assert.That(first.Disposed.IsCompleted,
                                  Is.False);
                      // Second one has been released synchronously by the caller
                      Assert.That(second.Disposed.IsCompleted,
                                  Is.True);
                    });
  }

  [Test]
  [Timeout(10000)]
  public async Task ParallelismShouldBeBounded()
  {
    const int parallelism = 3;
    const int nbMessages  = 20;

    using var releaser = CreateReleaser(parallelism: parallelism);
    await releaser.StartAsync(CancellationToken.None)
                  .ConfigureAwait(false);

    var tracker = new ConcurrencyTracker();
    var messages = Enumerable.Range(0,
                                    nbMessages)
                             .Select(_ => new CountingQueueMessageHandler(tracker,
                                                                          TimeSpan.FromMilliseconds(50)))
                             .ToList();

    foreach (var message in messages)
    {
      await new AsyncReleaseQueueMessageHandler(message,
                                                releaser).DisposeAsync()
                                                         .ConfigureAwait(false);
    }

    await Task.WhenAll(messages.Select(m => m.Disposed))
              .WaitAsync(TimeSpan.FromSeconds(5))
              .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(tracker.Max,
                                  Is.LessThanOrEqualTo(parallelism));
                      Assert.That(tracker.Max,
                                  Is.GreaterThan(1));
                    });
  }

  [Test]
  [Timeout(10000)]
  public async Task StopShouldReleaseEnqueuedMessages()
  {
    using var releaser = CreateReleaser(parallelism: 1);
    await releaser.StartAsync(CancellationToken.None)
                  .ConfigureAwait(false);

    var messages = Enumerable.Range(0,
                                    10)
                             .Select(_ => new CountingQueueMessageHandler(null,
                                                                          TimeSpan.FromMilliseconds(20)))
                             .ToList();
    foreach (var message in messages)
    {
      await new AsyncReleaseQueueMessageHandler(message,
                                                releaser).DisposeAsync()
                                                         .ConfigureAwait(false);
    }

    // Triggers the late cancellation token (no grace delay): the queue is closed
    lifetime_.StopApplication();

    await releaser.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(5))
                  .ConfigureAwait(false);

    Assert.That(messages.Select(m => m.DisposeCount),
                Is.All.EqualTo(1));

    // The queue is closed: new messages are released by the caller
    var late = new SimpleQueueMessageHandler();
    await new AsyncReleaseQueueMessageHandler(late,
                                              releaser).DisposeAsync()
                                                       .ConfigureAwait(false);
    Assert.That(late.Disposed.IsCompleted,
                Is.True);
  }

  [Test]
  [Timeout(10000)]
  public async Task ReleaseErrorShouldNotStopTheReleaser()
  {
    using var releaser = CreateReleaser(parallelism: 1);
    await releaser.StartAsync(CancellationToken.None)
                  .ConfigureAwait(false);

    await new AsyncReleaseQueueMessageHandler(new ThrowingQueueMessageHandler(),
                                              releaser).DisposeAsync()
                                                       .ConfigureAwait(false);

    var next = new SimpleQueueMessageHandler();
    await new AsyncReleaseQueueMessageHandler(next,
                                              releaser).DisposeAsync()
                                                       .ConfigureAwait(false);

    await next.Disposed.WaitAsync(TimeSpan.FromSeconds(5))
              .ConfigureAwait(false);
    Assert.That(releaser.ExecuteTask!.IsCompleted,
                Is.False);
  }

  private abstract class TestQueueMessageHandlerBase : IQueueMessageHandler
  {
    public abstract ValueTask DisposeAsync();

    [Obsolete("ArmoniK now manages loss of link with the queue")]
    public CancellationToken CancellationToken { get; set; }

    public string MessageId { get; } = Guid.NewGuid()
                                           .ToString();

    public string             TaskId            { get; } = "";
    public QueueMessageStatus Status            { get; set; }
    public DateTime           ReceptionDateTime { get; init; } = DateTime.UtcNow;
  }

  private sealed class BlockingQueueMessageHandler : TestQueueMessageHandlerBase
  {
    private readonly TaskCompletionSource completed_ = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource started_   = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource unblock_   = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Started
      => started_.Task;

    public Task Completed
      => completed_.Task;

    public void Unblock()
      => unblock_.TrySetResult();

    public override async ValueTask DisposeAsync()
    {
      started_.TrySetResult();
      await unblock_.Task.ConfigureAwait(false);
      completed_.TrySetResult();
    }
  }

  private sealed class CountingQueueMessageHandler : TestQueueMessageHandlerBase
  {
    private readonly TimeSpan             delay_;
    private readonly TaskCompletionSource disposed_ = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly ConcurrencyTracker?  tracker_;
    private          int                  disposeCount_;

    public CountingQueueMessageHandler(ConcurrencyTracker? tracker = null,
                                       TimeSpan            delay   = default)
    {
      tracker_ = tracker;
      delay_   = delay;
    }

    public int DisposeCount
      => disposeCount_;

    public Task Disposed
      => disposed_.Task;

    public override async ValueTask DisposeAsync()
    {
      tracker_?.Enter();
      try
      {
        if (delay_ > TimeSpan.Zero)
        {
          await Task.Delay(delay_)
                    .ConfigureAwait(false);
        }
      }
      finally
      {
        tracker_?.Exit();
      }

      Interlocked.Increment(ref disposeCount_);
      disposed_.TrySetResult();
    }
  }

  private sealed class ThrowingQueueMessageHandler : TestQueueMessageHandlerBase
  {
    public override ValueTask DisposeAsync()
      => throw new InvalidOperationException("Release failure");
  }

  private sealed class ConcurrencyTracker
  {
    private int current_;
    private int max_;

    public int Max
      => max_;

    public void Enter()
    {
      var current = Interlocked.Increment(ref current_);
      int max;
      do
      {
        max = max_;
      } while (current > max && Interlocked.CompareExchange(ref max_,
                                                            current,
                                                            max) != max);
    }

    public void Exit()
      => Interlocked.Decrement(ref current_);
  }
}
