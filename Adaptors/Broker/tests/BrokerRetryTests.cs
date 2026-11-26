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

using System.Net;
using System.Text;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Broker.Tests;

/// <summary>
///   Retries of the adapter against a scripted server, for the answers the real server cannot be made to give on demand.
/// </summary>
[TestFixture]
[NonParallelizable]
public class BrokerRetryTests
{
  [SetUp]
  public void SetUp()
  {
    port_     = BrokerIntegrationTests.FreePort();
    listener_ = new HttpListener();
    listener_.Prefixes.Add($"http://127.0.0.1:{port_}/");
    listener_.Start();
  }

  [TearDown]
  public void TearDown()
    => listener_?.Close();

  private HttpListener? listener_;
  private int           port_;

  /// <summary>
  ///   Answers the enqueue requests with the given statuses in turn, and records when each one arrived.
  /// </summary>
  private async Task<List<TimeSpan>> Serve(IReadOnlyList<(HttpStatusCode Status, string? RetryAfter)> answers)
  {
    var watch    = System.Diagnostics.Stopwatch.StartNew();
    var arrivals = new List<TimeSpan>();
    foreach (var (status, retryAfter) in answers)
    {
      var context = await listener_!.GetContextAsync();
      arrivals.Add(watch.Elapsed);
      var response = context.Response;
      response.StatusCode = (int)status;
      if (retryAfter is not null)
      {
        response.Headers["Retry-After"] = retryAfter;
      }

      if (status != HttpStatusCode.NoContent)
      {
        var body = Encoding.UTF8.GetBytes($$"""{"type":"urn:armonik:broker:overloaded","status":{{(int)status}},"retryable":true}""");
        response.ContentType = "application/problem+json";
        await response.OutputStream.WriteAsync(body);
      }

      response.Close();
    }

    return arrivals;
  }

  private IPushQueueStorage BuildPush(ServiceCollection services)
  {
    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(new Dictionary<string, string?>
                                        {
                                          ["Broker:Endpoint"]         = $"http://127.0.0.1:{port_}",
                                          ["Broker:Http2"]            = "false",
                                          ["Broker:MaxRetryDuration"] = "00:00:20",
                                        });
    services.AddLogging();
    new QueueBuilder().Build(services,
                             configuration,
                             NullLogger.Instance);
    return services.BuildServiceProvider()
                   .GetRequiredService<IPushQueueStorage>();
  }

  [Test]
  public async Task RetryAfterIsRespected()
  {
    var push = BuildPush(new ServiceCollection());
    var server = Serve([
                         (HttpStatusCode.ServiceUnavailable, "1"),
                         (HttpStatusCode.NoContent, null),
                       ]);
    await push.PushMessagesAsync([
                                   new MessageData("t1",
                                                   "s1",
                                                   new TaskOptions
                                                   {
                                                     Priority    = 1,
                                                     PartitionId = "part",
                                                   }),
                                 ],
                                 "part")
              .WaitAsync(TimeSpan.FromSeconds(10));
    var arrivals = await server;
    Assert.That(arrivals[1] - arrivals[0],
                Is.GreaterThanOrEqualTo(TimeSpan.FromMilliseconds(900)),
                "the server asked to wait one second before retrying");
  }
}
