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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Pollster;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture]
public class DataPrefetcherTest
{
  [SetUp]
  public void SetUp()
    => activitySource_ = new ActivitySource(nameof(DataPrefetcherTest));

  [TearDown]
  public virtual void TearDown()
  {
  }

  private ActivitySource? activitySource_;

  [Test]
  public async Task EmptyPayloadAndOneDependency()
  {
    const string sessionId   = "SessionId";
    const string dependency1 = "Dependency1";
    const string payloadId   = "PayloadId";

    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<byte[]>(),
                                                  It.IsAny<CancellationToken>()))
                     .Returns((byte[]            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            new Injection.Options.Pollster(),
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());


    var sharedFolder = CreateTempFolder();

    await dataPrefetcher.PrefetchDataAsync(new Dictionary<string, byte[]>
                                           {
                                             {
                                               payloadId, "payload"u8.ToArray()
                                             },
                                             {
                                               dependency1, "dependency1"u8.ToArray()
                                             },
                                           },
                                           sharedFolder,
                                           CancellationToken.None)
                        .ConfigureAwait(false);

    Assert.That(File.Exists(Path.Combine(sharedFolder,
                                         payloadId)),
                Is.True);
    Assert.That(File.Exists(Path.Combine(sharedFolder,
                                         dependency1)),
                Is.True);

    Directory.Delete(sharedFolder,
                     true);
  }


  [Test]
  public async Task EmptyPayloadAndNoDependenciesStateMachine()
  {
    const string sessionId   = "SessionId";
    const string dependency1 = "Dependency1";
    const string dependency2 = "Dependency2";
    const string payloadId   = "PayloadId";


    var mockObjectStorage = new Mock<IObjectStorage>();
    mockObjectStorage.Setup(x => x.GetValuesAsync(It.IsAny<byte[]>(),
                                                  It.IsAny<CancellationToken>()))
                     .Returns((byte[]            _,
                               CancellationToken _) => new List<byte[]>
                                                       {
                                                         Convert.FromBase64String("1111"),
                                                         Convert.FromBase64String("2222"),
                                                         Convert.FromBase64String("3333"),
                                                         Convert.FromBase64String("4444"),
                                                       }.ToAsyncEnumerable());

    var loggerFactory = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            new Injection.Options.Pollster(),
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());


    var sharedFolder = CreateTempFolder();

    await dataPrefetcher.PrefetchDataAsync(new Dictionary<string, byte[]>
                                           {
                                             {
                                               payloadId, "payload"u8.ToArray()
                                             },
                                           },
                                           sharedFolder,
                                           CancellationToken.None)
                        .ConfigureAwait(false);

    Assert.That(File.Exists(Path.Combine(sharedFolder,
                                         payloadId)),
                Is.True);

    Directory.Delete(sharedFolder,
                     true);
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    var mockObjectStorage = new Mock<IObjectStorage>();
    var loggerFactory     = new LoggerFactory();

    var dataPrefetcher = new DataPrefetcher(mockObjectStorage.Object,
                                            new Injection.Options.Pollster(),
                                            activitySource_,
                                            loggerFactory.CreateLogger<DataPrefetcher>());

    Assert.That(await dataPrefetcher.Check(HealthCheckTag.Liveness)
                                    .ConfigureAwait(false),
                Is.Not.EqualTo(HealthCheckResult.Healthy()));
    Assert.That(await dataPrefetcher.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false),
                Is.Not.EqualTo(HealthCheckResult.Healthy()));
    Assert.That(await dataPrefetcher.Check(HealthCheckTag.Startup)
                                    .ConfigureAwait(false),
                Is.Not.EqualTo(HealthCheckResult.Healthy()));

    await dataPrefetcher.Init(CancellationToken.None)
                        .ConfigureAwait(false);

    Assert.That(await dataPrefetcher.Check(HealthCheckTag.Liveness)
                                    .ConfigureAwait(false),
                Is.EqualTo(HealthCheckResult.Healthy()));
    Assert.That(await dataPrefetcher.Check(HealthCheckTag.Readiness)
                                    .ConfigureAwait(false),
                Is.EqualTo(HealthCheckResult.Healthy()));
    Assert.That(await dataPrefetcher.Check(HealthCheckTag.Startup)
                                    .ConfigureAwait(false),
                Is.EqualTo(HealthCheckResult.Healthy()));
  }

  private static DataPrefetcher CreatePrefetcher(IObjectStorage objectStorage,
                                                 int            parallelism)
    => new(objectStorage,
           new Injection.Options.Pollster
           {
             DataPrefetchParallelism = parallelism,
           },
           null,
           new LoggerFactory().CreateLogger<DataPrefetcher>());

  private static string CreateTempFolder()
    => Directory.CreateDirectory(Path.Combine(Path.GetTempPath(),
                                              $"{nameof(DataPrefetcherTest)}-{Guid.NewGuid()}"))
                .FullName;

  private static Dictionary<string, byte[]> OpaqueIds(int count)
    => Enumerable.Range(0,
                        count)
                 .ToDictionary(i => $"result{i}",
                               i => Encoding.UTF8.GetBytes($"opaque{i}"));

  /// <summary>
  ///   Object storage whose downloads stay in flight until <see cref="Release" /> is completed,
  ///   recording how many of them run concurrently.
  /// </summary>
  private sealed class BlockingObjectStorage
  {
    private readonly object lock_ = new();
    private          int    current_;

    public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource<int> Reached { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public int Target { get; init; }

    public byte[]? Failing { get; init; }

    public int InFlight
    {
      get
      {
        lock (lock_)
        {
          return current_;
        }
      }
    }

    public int MaxConcurrency { get; private set; }

    public Mock<IObjectStorage> Mock()
    {
      var mock = new Mock<IObjectStorage>();
      mock.Setup(x => x.GetValuesAsync(It.IsAny<byte[]>(),
                                       It.IsAny<CancellationToken>()))
          .Returns((byte[]            id,
                    CancellationToken token) => Download(id,
                                                         token));
      return mock;
    }

    private async IAsyncEnumerable<byte[]> Download(byte[]                                     id,
                                                    [EnumeratorCancellation] CancellationToken token)
    {
      lock (lock_)
      {
        current_++;
        MaxConcurrency = Math.Max(MaxConcurrency,
                                  current_);
        if (current_ >= Target)
        {
          Reached.TrySetResult(current_);
        }
      }

      try
      {
        if (Failing is not null && id.SequenceEqual(Failing))
        {
          await Reached.Task.WaitAsync(token)
                       .ConfigureAwait(false);
          throw new ObjectDataNotFoundException("missing");
        }

        await Release.Task.WaitAsync(token)
                     .ConfigureAwait(false);
        yield return id;
      }
      finally
      {
        lock (lock_)
        {
          current_--;
        }
      }
    }
  }

  [Test]
  [CancelAfter(10000)]
  public async Task DownloadsShouldRunConcurrently(CancellationToken cancellationToken)
  {
    const int count = 4;
    var storage = new BlockingObjectStorage
                  {
                    Target = count,
                  };
    var prefetcher = CreatePrefetcher(storage.Mock()
                                             .Object,
                                      -1);
    var folder    = CreateTempFolder();
    var opaqueIds = OpaqueIds(count);

    var prefetch = prefetcher.PrefetchDataAsync(opaqueIds,
                                                folder,
                                                cancellationToken);

    // With sequential downloads, the first one would never complete and this would time out.
    Assert.That(await storage.Reached.Task.WaitAsync(cancellationToken)
                             .ConfigureAwait(false),
                Is.EqualTo(count));

    storage.Release.SetResult();
    await prefetch.ConfigureAwait(false);

    foreach (var (resultId, opaqueId) in opaqueIds)
    {
      Assert.That(await File.ReadAllBytesAsync(Path.Combine(folder,
                                                            resultId),
                                               cancellationToken)
                            .ConfigureAwait(false),
                  Is.EqualTo(opaqueId));
    }

    Directory.Delete(folder,
                     true);
  }

  [Test]
  [CancelAfter(10000)]
  public async Task DownloadsShouldRespectParallelismLimit(CancellationToken cancellationToken)
  {
    const int limit = 2;
    var storage = new BlockingObjectStorage
                  {
                    Target = limit,
                  };
    var prefetcher = CreatePrefetcher(storage.Mock()
                                             .Object,
                                      limit);
    var folder = CreateTempFolder();

    var prefetch = prefetcher.PrefetchDataAsync(OpaqueIds(6),
                                                folder,
                                                cancellationToken);

    await storage.Reached.Task.WaitAsync(cancellationToken)
                 .ConfigureAwait(false);
    // Leave room for an extra download to start if the limit were not enforced
    await Task.Delay(100,
                     cancellationToken)
              .ConfigureAwait(false);
    storage.Release.SetResult();
    await prefetch.ConfigureAwait(false);

    Assert.That(storage.MaxConcurrency,
                Is.EqualTo(limit));
    Assert.That(Directory.GetFiles(folder),
                Has.Length.EqualTo(6));

    Directory.Delete(folder,
                     true);
  }

  [Test]
  [CancelAfter(10000)]
  public void FailureShouldCancelOtherDownloadsAndRethrowRootCause(CancellationToken cancellationToken)
  {
    const int count     = 4;
    var       opaqueIds = OpaqueIds(count);
    var storage = new BlockingObjectStorage
                  {
                    Target = count,
                    // Last in enumeration order, so the cancelled downloads come first in Task.WhenAll
                    Failing = opaqueIds[$"result{count - 1}"],
                  };
    var prefetcher = CreatePrefetcher(storage.Mock()
                                             .Object,
                                      -1);
    var folder = CreateTempFolder();

    // The caller token is never cancelled and Release is never completed: only the cancellation triggered by the
    // failure can end the other downloads, otherwise the wait times out.
    // TaskHandler relies on the exact type to fail the task instead of resubmitting it.
    Assert.That(() => prefetcher.PrefetchDataAsync(opaqueIds,
                                                   folder,
                                                   CancellationToken.None)
                                .WaitAsync(TimeSpan.FromSeconds(5),
                                           cancellationToken),
                Throws.TypeOf<ObjectDataNotFoundException>());
    Assert.That(storage.InFlight,
                Is.Zero);

    Directory.Delete(folder,
                     true);
  }

  [Test]
  [CancelAfter(10000)]
  public void FailureShouldCancelDownloadsWaitingForASlot(CancellationToken cancellationToken)
  {
    const int count     = 4;
    var       opaqueIds = OpaqueIds(count);
    var storage = new BlockingObjectStorage
                  {
                    Target = 1,
                    // First in enumeration order, so it takes the only slot and the others wait for it
                    Failing = opaqueIds["result0"],
                  };
    var prefetcher = CreatePrefetcher(storage.Mock()
                                             .Object,
                                      1);
    var folder = CreateTempFolder();

    Assert.That(() => prefetcher.PrefetchDataAsync(opaqueIds,
                                                   folder,
                                                   CancellationToken.None)
                                .WaitAsync(TimeSpan.FromSeconds(5),
                                           cancellationToken),
                Throws.TypeOf<ObjectDataNotFoundException>());
    Assert.That(storage.MaxConcurrency,
                Is.EqualTo(1));
    // The waiting downloads were cancelled before acquiring the slot, so they never created their file
    Assert.That(Directory.GetFiles(folder)
                         .Select(Path.GetFileName),
                Is.EquivalentTo(new[]
                                {
                                  "result0",
                                }));

    Directory.Delete(folder,
                     true);
  }

  [Test]
  [CancelAfter(10000)]
  public async Task CallerCancellationShouldStopAllDownloads(CancellationToken cancellationToken)
  {
    const int limit = 2;
    var storage = new BlockingObjectStorage
                  {
                    Target = limit,
                  };
    var prefetcher = CreatePrefetcher(storage.Mock()
                                             .Object,
                                      limit);
    var folder = CreateTempFolder();

    using var cts = new CancellationTokenSource();
    var prefetch = prefetcher.PrefetchDataAsync(OpaqueIds(5),
                                                folder,
                                                cts.Token);

    await storage.Reached.Task.WaitAsync(cancellationToken)
                 .ConfigureAwait(false);
    // Two downloads are in flight and three wait for a slot when the caller cancels
    await cts.CancelAsync()
             .ConfigureAwait(false);

    // TaskHandler resubmits the task on any exception other than ObjectDataNotFoundException
    Assert.That(() => prefetch.WaitAsync(TimeSpan.FromSeconds(5),
                                         cancellationToken),
                Throws.InstanceOf<OperationCanceledException>());
    Assert.That(storage.InFlight,
                Is.Zero);
    Assert.That(Directory.GetFiles(folder),
                Has.Length.EqualTo(limit));

    Directory.Delete(folder,
                     true);
  }

  [Test]
  public async Task ExistingFileShouldBeOverwritten()
  {
    var mock = new Mock<IObjectStorage>();
    mock.Setup(x => x.GetValuesAsync(It.IsAny<byte[]>(),
                                     It.IsAny<CancellationToken>()))
        .Returns((byte[]            _,
                  CancellationToken _) => new List<byte[]>
                                          {
                                            "ab"u8.ToArray(),
                                            "cd"u8.ToArray(),
                                          }.ToAsyncEnumerable());

    var prefetcher = CreatePrefetcher(mock.Object,
                                      0);
    var folder = CreateTempFolder();
    var path = Path.Combine(folder,
                            "result");
    await File.WriteAllTextAsync(path,
                                 "a longer leftover from a previous attempt")
              .ConfigureAwait(false);

    await prefetcher.PrefetchDataAsync(new Dictionary<string, byte[]>
                                       {
                                         {
                                           "result", "opaque"u8.ToArray()
                                         },
                                       },
                                       folder,
                                       CancellationToken.None)
                    .ConfigureAwait(false);

    Assert.That(await File.ReadAllTextAsync(path)
                          .ConfigureAwait(false),
                Is.EqualTo("abcd"));

    Directory.Delete(folder,
                     true);
  }
}
