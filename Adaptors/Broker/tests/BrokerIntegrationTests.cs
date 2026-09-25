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

using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Broker.Tests;

/// <summary>
///   Runs the adapter against the real Rust server. The binary is found through BROKER_BINARY or the
///   cargo target directories; the tests are ignored when it has not been built.
/// </summary>
[TestFixture]
[NonParallelizable]
public class BrokerIntegrationTests
{
  [SetUp]
  public async Task SetUp()
  {
    binary_ = FindBinary();
    if (binary_ is null)
    {
      Assert.Ignore("armonik-broker binary not built (cargo build in Broker/, or set BROKER_BINARY)");
    }

    port_ = FreePort();
    await StartServer();
    (push_, pull_, provider_) = await BuildAdapter();
  }

  [TearDown]
  public async Task TearDown()
  {
    StopServer();
    if (provider_ is not null)
    {
      await provider_.DisposeAsync();
    }
  }

  private string?          binary_;
  private int              port_;
  private Process?         server_;
  private IPushQueueStorage push_     = null!;
  private IPullQueueStorage pull_     = null!;
  private ServiceProvider?  provider_;

  internal static string? FindBinary()
  {
    var env = Environment.GetEnvironmentVariable("BROKER_BINARY");
    if (!string.IsNullOrEmpty(env))
    {
      return File.Exists(env)
               ? env
               : null;
    }

    var candidates = new List<string>();
    var home       = Environment.GetEnvironmentVariable("HOME");
    if (home is not null)
    {
      candidates.Add(Path.Combine(home,
                                  ".cache",
                                  "armonik-broker-target",
                                  "debug",
                                  "armonik-broker"));
    }

    var dir = new DirectoryInfo(TestContext.CurrentContext.TestDirectory);
    while (dir is not null && !File.Exists(Path.Combine(dir.FullName,
                                                        "ArmoniK.Core.sln")))
    {
      dir = dir.Parent;
    }

    if (dir is not null)
    {
      candidates.Add(Path.Combine(dir.FullName,
                                  "Broker",
                                  "target",
                                  "debug",
                                  "armonik-broker"));
    }

    return candidates.FirstOrDefault(File.Exists);
  }

  internal static int FreePort()
  {
    using var l = new TcpListener(IPAddress.Loopback,
                                  0);
    l.Start();
    return ((IPEndPoint)l.LocalEndpoint).Port;
  }

  private async Task StartServer(IDictionary<string, string>? environment = null)
  {
    var info = new ProcessStartInfo(binary_!)
               {
                 RedirectStandardOutput = true,
                 RedirectStandardError  = true,
               };
    info.Environment["BROKER_LISTEN"] = $"127.0.0.1:{port_}";
    info.Environment["BROKER_LEASE_MS"] = "3000";
    foreach (var (key, value) in environment ?? new Dictionary<string, string>())
    {
      info.Environment[key] = value;
    }

    server_ = Process.Start(info)!;
    server_.BeginOutputReadLine();
    server_.BeginErrorReadLine();
    using var http = new HttpClient();
    for (var i = 0; i < 100; i++)
    {
      try
      {
        if ((await http.GetAsync($"http://127.0.0.1:{port_}/v1/health")).IsSuccessStatusCode)
        {
          return;
        }
      }
      catch (HttpRequestException)
      {
      }

      await Task.Delay(50);
    }

    Assert.Fail("broker did not start");
  }

  private void StopServer()
  {
    if (server_ is { HasExited: false })
    {
      server_.Kill();
      server_.WaitForExit();
    }

    server_?.Dispose();
    server_ = null;
  }

  private async Task<(IPushQueueStorage, IPullQueueStorage, ServiceProvider)> BuildAdapter()
  {
    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(new Dictionary<string, string?>
                                        {
                                          ["Broker:Endpoint"]         = $"http://127.0.0.1:{port_}",
                                          ["Broker:PullWait"]         = "00:00:01",
                                          ["Broker:RenewPeriod"]      = "00:00:01",
                                          ["Broker:MaxRetryDuration"] = "00:00:20",
                                          ["Broker:NodeId"]           = "node-a",
                                          ["Broker:Affinity"]         = "true",
                                        });
    var services = new ServiceCollection();
    services.AddLogging(b => b.SetMinimumLevel(LogLevel.Warning));
    new QueueBuilder().Build(services,
                             configuration,
                             NullLogger.Instance);
    var provider = services.BuildServiceProvider();
    var push     = provider.GetRequiredService<IPushQueueStorage>();
    var pull     = provider.GetRequiredService<IPullQueueStorage>();
    await push.Init(CancellationToken.None);
    await pull.Init(CancellationToken.None);
    return (push, pull, provider);
  }

  private static MessageData Message(string id,
                                     string session,
                                     int    priority = 1)
    => new(id,
           session,
           new TaskOptions
           {
             Priority    = priority,
             PartitionId = "part",
           });

  private async Task<List<IQueueMessageHandler>> PullAll(int max,
                                                         int attempts = 5)
  {
    var got = new List<IQueueMessageHandler>();
    for (var i = 0; i < attempts && got.Count < max; i++)
    {
      await foreach (var h in pull_.PullMessagesAsync("part",
                                                     max - got.Count))
      {
        got.Add(h);
      }
    }

    return got;
  }

  [Test]
  public async Task AffinityIsOptIn()
  {
    Assert.That(push_.UsesDataDependencies,
                Is.True,
                "enabled in this fixture");
    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(new Dictionary<string, string?>
                                        {
                                          ["Broker:Endpoint"] = $"http://127.0.0.1:{port_}",
                                        });
    var services = new ServiceCollection();
    services.AddLogging();
    new QueueBuilder().Build(services,
                             configuration,
                             NullLogger.Instance);
    await using var provider = services.BuildServiceProvider();
    Assert.That(provider.GetRequiredService<IPushQueueStorage>()
                        .UsesDataDependencies,
                Is.False,
                "no dependency sizes are read unless affinity is configured");
  }

  [Test]
  public async Task ClientFollowsTheServerLimits()
  {
    // Batches of at most (2048 - 256) / 400 = 4 items and pulls of at most 2 messages.
    StopServer();
    await provider_!.DisposeAsync();
    await StartServer(new Dictionary<string, string>
                      {
                        ["BROKER_MAX_PULL"]       = "2",
                        ["BROKER_MAX_BODY_BYTES"] = "2048",
                      });
    (push_, pull_, provider_) = await BuildAdapter();

    await push_.PushMessagesAsync(Enumerable.Range(0,
                                                   20)
                                            .Select(i => Message($"t{i}",
                                                                 "s1")),
                                  "part");
    var got = new List<IQueueMessageHandler>();
    for (var i = 0; i < 20 && got.Count < 20; i++)
    {
      var batch = new List<IQueueMessageHandler>();
      await foreach (var h in pull_.PullMessagesAsync("part",
                                                     5))
      {
        batch.Add(h);
      }

      Assert.That(batch,
                  Has.Count.LessThanOrEqualTo(2));
      got.AddRange(batch);
    }

    Assert.That(got.Select(h => h.TaskId),
                Is.EquivalentTo(Enumerable.Range(0,
                                                 20)
                                          .Select(i => $"t{i}")));
    foreach (var h in got)
    {
      h.Status = QueueMessageStatus.Processed;
      await h.DisposeAsync();
    }
  }

  [Test]
  public async Task PushPullAckCycle()
  {
    Assert.That(push_.MaxPriority,
                Is.EqualTo(16));
    await push_.PushMessagesAsync(Enumerable.Range(0,
                                                   300)
                                            .Select(i => Message($"t{i}",
                                                                 "s1")),
                                  "part");
    var got = await PullAll(300,
                            400);
    Assert.That(got.Select(h => h.TaskId),
                Is.EquivalentTo(Enumerable.Range(0,
                                                 300)
                                          .Select(i => $"t{i}")));
    foreach (var h in got)
    {
      h.Status = QueueMessageStatus.Processed;
      await h.DisposeAsync();
    }

    Assert.That(await PullAll(1,
                              1),
                Is.Empty);
  }

  [Test]
  public async Task PostponedMessagesComeBack()
  {
    await push_.PushMessagesAsync([Message("t", "s1")],
                                  "part");
    var first = (await PullAll(1)).Single();
    first.Status = QueueMessageStatus.Postponed;
    await first.DisposeAsync();
    var again = (await PullAll(1)).Single();
    Assert.That(again.TaskId,
                Is.EqualTo("t"));
    Assert.That(again.MessageId,
                Is.Not.EqualTo(first.MessageId),
                "a new distribution has a new token");
    again.Status = QueueMessageStatus.Processed;
    await again.DisposeAsync();
  }

  [Test]
  public async Task SessionsAreServedInTurn()
  {
    await push_.PushMessagesAsync(Enumerable.Range(0,
                                                   20)
                                            .Select(i => Message($"big{i}",
                                                                 "big",
                                                                 16)),
                                  "part");
    await push_.PushMessagesAsync(Enumerable.Range(0,
                                                   2)
                                            .Select(i => Message($"small{i}",
                                                                 "small")),
                                  "part");
    var order = new List<string>();
    for (var i = 0; i < 4; i++)
    {
      var h = (await PullAll(1)).Single();
      order.Add(h.TaskId);
      h.Status = QueueMessageStatus.Processed;
      await h.DisposeAsync();
    }

    Assert.That(order.Count(t => t.StartsWith("small")),
                Is.EqualTo(2),
                string.Join(",",
                            order));
  }

  [Test]
  public async Task PullSurvivesServerRestart()
  {
    await push_.PushMessagesAsync([Message("before", "s1")],
                                  "part");
    var held = (await PullAll(1)).Single();

    StopServer();
    // The server is down: pull returns nothing and does not throw, health stays good.
    Assert.That(await PullAll(1,
                              1),
                Is.Empty);
    Assert.That((await pull_.Check(HealthCheckTag.Liveness)).Status,
                Is.EqualTo(Microsoft.Extensions.Diagnostics.HealthChecks.HealthStatus.Healthy));

    await StartServer();
    // The token of the previous epoch is acknowledged without error.
    held.Status = QueueMessageStatus.Processed;
    await held.DisposeAsync();

    await push_.PushMessagesAsync([Message("after", "s1")],
                                  "part");
    var got = await PullAll(1,
                            10);
    Assert.That(got.Single()
                   .TaskId,
                Is.EqualTo("after"));
  }

  [Test]
  public async Task HeldMessagesOutliveTheirLease()
  {
    // The test server lease is 3 s; the client renews what it holds every second.
    await push_.PushMessagesAsync([Message("long", "s1")],
                                  "part");
    var held = (await PullAll(1)).Single();
    await Task.Delay(TimeSpan.FromSeconds(5));
    Assert.That(await PullAll(1,
                              1),
                Is.Empty,
                "a held message is renewed, not redelivered");
    held.Status = QueueMessageStatus.Processed;
    await held.DisposeAsync();
  }

  [Test]
  public async Task OutputsAreAcceptedAtAck()
  {
    await push_.PushMessagesAsync([
                                    Message("producer",
                                            "s1") with
                                    {
                                      Dependencies = [("in", 10)],
                                    },
                                  ],
                                  "part");
    var h = (await PullAll(1)).Single();
    ((IDataAffinityMessageHandler)h).SetOutputs([("out", 1L << 20)]);
    h.Status = QueueMessageStatus.Processed;
    await h.DisposeAsync();
    Assert.That(await PullAll(1,
                              1),
                Is.Empty);
  }
}
