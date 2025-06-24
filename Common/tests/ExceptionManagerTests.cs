// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Utils;

using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class ExceptionManagerTests
{
  [OneTimeSetUp]
  public void OneTimeSetUp()
  {
    loggerFactory_ = new LoggerFactory();
    loggerFactory_.AddProvider(new ConsoleForwardingLoggerProvider());
  }

  [SetUp]
  public void Setup()
    => lifetime_ = new ApplicationLifetime(loggerFactory_.CreateLogger<ApplicationLifetime>());

  [TearDown]
  public void TearDown()
    => lifetime_.StopApplication();

  private ApplicationLifetime lifetime_;
  private ILoggerFactory      loggerFactory_;

  [Test]
  [Timeout(100)]
  public void NoDelay()
  {
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(TimeSpan.Zero,
                                                                     0));

    Assert.That(em.EarlyCancellationToken,
                Is.EqualTo(em.LateCancellationToken));
  }

  [Test]
  [Timeout(1000)]
  public async Task NominalStop([Values(0,
                                        1,
                                        10,
                                        null)]
                                int? maxError)
  {
    var logger = maxError is null
                   ? NullLogger.Instance
                   : loggerFactory_.CreateLogger(nameof(NominalStop));
    var events = new ConcurrentQueue<int>();
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(TimeSpan.FromSeconds(5),
                                                                     maxError));

    Assert.That(em.EarlyCancellationToken,
                Is.Not.EqualTo(em.LateCancellationToken));

    await using var d0 = lifetime_.ApplicationStarted.Register(() => events.Enqueue(0));
    await using var d1 = lifetime_.ApplicationStopping.Register(() => events.Enqueue(1));
    await using var d2 = em.EarlyCancellationToken.Register(() => events.Enqueue(2));
    await using var d3 = lifetime_.ApplicationStopped.Register(() => events.Enqueue(3));
    await using var d4 = em.LateCancellationToken.Register(() => events.Enqueue(4));

    lifetime_.NotifyStarted();

    await Task.Delay(10)
              .ConfigureAwait(false);

    for (var i = 0; i < (maxError ?? 100000); i++)
    {
      em.RecordError(logger,
                     new ApplicationException($"Error {i}"),
                     nameof(NominalStop));
    }

    Assert.That(em.Failed,
                Is.False);

    lifetime_.StopApplication();

    try
    {
      await em.EarlyCancellationToken.AsTask()
              .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // ignore
    }

    Assert.That(em.Failed,
                Is.False);

    lifetime_.NotifyStopped();

    Assert.That(events,
                Is.EqualTo(Enumerable.Range(0,
                                            5)));
  }

  [Test]
  [Timeout(10000)]
  [Retry(2)] // Sometimes on Windows, the delay is not respected
  public async Task GraceDelayStop([Values(0,
                                           5)]
                                   int maxError)
  {
    var logger = loggerFactory_.CreateLogger(nameof(NominalStop));
    var events = new ConcurrentQueue<int>();
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(TimeSpan.FromSeconds(1),
                                                                     maxError));

    Assert.That(em.EarlyCancellationToken,
                Is.Not.EqualTo(em.LateCancellationToken));

    await using var d0 = lifetime_.ApplicationStarted.Register(() => events.Enqueue(0));
    await using var d1 = lifetime_.ApplicationStopping.Register(() => events.Enqueue(1));
    await using var d2 = em.EarlyCancellationToken.Register(() => events.Enqueue(2));
    await using var d3 = em.LateCancellationToken.Register(() => events.Enqueue(3));
    await using var d4 = lifetime_.ApplicationStopped.Register(() => events.Enqueue(4));

    lifetime_.NotifyStarted();

    await Task.Delay(10)
              .ConfigureAwait(false);

    for (var i = 0; i < maxError; i++)
    {
      em.RecordError(logger,
                     new ApplicationException($"Error {i}"),
                     nameof(NominalStop));
    }

    Assert.That(em.Failed,
                Is.False);

    var sw = Stopwatch.StartNew();
    lifetime_.StopApplication();

    try
    {
      await em.LateCancellationToken.AsTask()
              .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // ignore
    }

    var elapsed = sw.Elapsed.TotalSeconds;
    Assert.That(elapsed,
                Is.GreaterThanOrEqualTo(0.5));

    Assert.That(em.Failed,
                Is.False);

    lifetime_.NotifyStopped();

    Assert.That(events,
                Is.EqualTo(Enumerable.Range(0,
                                            5)));
  }

  [Test]
  [Timeout(10000)]
  [Sequential]
  public async Task ErrorStop([Values(0,
                                      5)]
                              int maxError,
                              [Values(1,
                                      2)]
                              int extraError)
  {
    var logger = loggerFactory_.CreateLogger(nameof(ErrorStop));
    var events = new ConcurrentQueue<int>();
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(TimeSpan.FromSeconds(1),
                                                                     maxError));

    Assert.That(em.EarlyCancellationToken,
                Is.Not.EqualTo(em.LateCancellationToken));

    await using var d0 = lifetime_.ApplicationStarted.Register(() => events.Enqueue(0));
    await using var d1 = em.EarlyCancellationToken.Register(() => events.Enqueue(1));
    await using var d2 = em.LateCancellationToken.Register(() => events.Enqueue(2));
    await using var d3 = lifetime_.ApplicationStopping.Register(() => events.Enqueue(3));
    await using var d4 = lifetime_.ApplicationStopped.Register(() => events.Enqueue(4));

    lifetime_.NotifyStarted();

    await Task.Delay(10)
              .ConfigureAwait(false);

    for (var i = 0; i < maxError + extraError; i++)
    {
      em.RecordError(logger,
                     new ApplicationException($"Error {i}"),
                     nameof(ErrorStop));
    }

    Assert.That(em.Failed,
                Is.True);

    try
    {
      await lifetime_.ApplicationStopping.AsTask()
                     .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // ignore
    }

    Assert.That(em.Failed,
                Is.True);

    lifetime_.NotifyStopped();

    Assert.That(events,
                Is.EqualTo(Enumerable.Range(0,
                                            5)));
  }

  [Test]
  [Timeout(10000)]
  public async Task FatalStop([Values(1,
                                      2)]
                              int nbFatal)
  {
    var logger = loggerFactory_.CreateLogger(nameof(FatalStop));
    var events = new ConcurrentQueue<int>();
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(TimeSpan.FromMilliseconds(15)));

    Assert.That(em.EarlyCancellationToken,
                Is.Not.EqualTo(em.LateCancellationToken));

    await using var d0 = lifetime_.ApplicationStarted.Register(() => events.Enqueue(0));
    await using var d1 = em.EarlyCancellationToken.Register(() => events.Enqueue(1));
    await using var d2 = em.LateCancellationToken.Register(() => events.Enqueue(2));
    await using var d3 = lifetime_.ApplicationStopping.Register(() => events.Enqueue(3));
    await using var d4 = lifetime_.ApplicationStopped.Register(() => events.Enqueue(4));

    lifetime_.NotifyStarted();

    await Task.Delay(10)
              .ConfigureAwait(false);

    for (var i = 0; i < nbFatal; i++)
    {
      em.FatalError(logger,
                    new ApplicationException($"Fatal {i}"),
                    nameof(FatalStop));
    }

    Assert.That(em.Failed,
                Is.True);

    em.RecordError(logger,
                   new ApplicationException("Error"),
                   nameof(FatalStop));

    try
    {
      await lifetime_.ApplicationStopping.AsTask()
                     .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // ignore
    }

    Assert.That(em.Failed,
                Is.True);

    lifetime_.NotifyStopped();

    Assert.That(events,
                Is.EqualTo(Enumerable.Range(0,
                                            5)));
  }

  [Test]
  [Timeout(10000)]
  public async Task Stop()
  {
    var logger = loggerFactory_.CreateLogger(nameof(Stop));
    var events = new ConcurrentQueue<int>();
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(TimeSpan.FromSeconds(15)));

    Assert.That(em.EarlyCancellationToken,
                Is.Not.EqualTo(em.LateCancellationToken));

    await using var d0 = lifetime_.ApplicationStarted.Register(() => events.Enqueue(0));
    await using var d1 = em.EarlyCancellationToken.Register(() => events.Enqueue(1));
    await using var d2 = lifetime_.ApplicationStopping.Register(() => events.Enqueue(2));
    await using var d3 = em.LateCancellationToken.Register(() => events.Enqueue(3));
    await using var d4 = lifetime_.ApplicationStopped.Register(() => events.Enqueue(4));

    lifetime_.NotifyStarted();
    em.Register();

    await Task.Delay(10)
              .ConfigureAwait(false);

    em.Stop(logger,
            "");

    try
    {
      await em.LateCancellationToken.AsTask()
              .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // ignore
    }

    Assert.That(em.Failed,
                Is.False);

    lifetime_.NotifyStopped();

    Assert.That(events,
                Is.EqualTo(Enumerable.Range(0,
                                            5)));
  }

  [Test]
  [Timeout(10000)]
  public async Task ConcurrentError()
  {
    var maxError = 10000;

    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(MaxError: maxError));

    await Enumerable.Range(0,
                           maxError)
                    .ParallelForEach(new ParallelTaskOptions(-1),
                                     i =>
                                     {
                                       em.RecordError(NullLogger.Instance,
                                                      new ApplicationException(),
                                                      i.ToString());
                                       return Task.CompletedTask;
                                     })
                    .ConfigureAwait(false);

    Assert.That(em.Failed,
                Is.False);

    em.RecordError(null,
                   new ApplicationException(),
                   "Fail");

    Assert.That(em.Failed,
                Is.True);
  }

  [Test]
  [Timeout(1000)]
  [Sequential]
  public async Task Success([Values(1,
                                    10,
                                    10)]
                            int maxError,
                            [Values(1,
                                    10,
                                    20)]
                            int nbSuccess)
  {
    var logger = loggerFactory_.CreateLogger(nameof(ErrorStop));
    using var em = new ExceptionManager(lifetime_,
                                        loggerFactory_.CreateLogger<ExceptionManager>(),
                                        new ExceptionManager.Options(MaxError: maxError));

    lifetime_.NotifyStarted();

    for (var i = 0; i < maxError; i++)
    {
      em.RecordError(logger,
                     new ApplicationException($"Error {i}"),
                     "First Error Loop");
    }

    Assert.That(em.Failed,
                Is.False);

    for (var i = 0; i < maxError; i++)
    {
      em.RecordSuccess();
    }

    Assert.That(em.Failed,
                Is.False);

    for (var i = 0; i < maxError; i++)
    {
      em.RecordError(logger,
                     new ApplicationException($"Error {i}"),
                     "Second Error Loop");
    }

    Assert.That(em.Failed,
                Is.False);

    em.RecordError(logger,
                   new ApplicationException("Final Error"),
                   "Third Error Loop");

    Assert.That(em.Failed,
                Is.True);

    em.RecordSuccess();

    Assert.That(em.Failed,
                Is.True);

    try
    {
      await lifetime_.ApplicationStopping.AsTask()
                     .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      // ignore
    }

    Assert.That(em.Failed,
                Is.True);

    lifetime_.NotifyStopped();
  }
}
