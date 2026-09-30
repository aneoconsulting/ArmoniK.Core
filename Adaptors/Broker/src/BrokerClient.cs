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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

internal sealed record PulledMessage(string Token,
                                     string TaskId);

/// <summary>
///   REST client of the broker (protocol v1). Stateless towards the server: it holds the tokens it
///   received and renews them all in one call. Transient failures are retried by <see cref="RetryHandler" />.
/// </summary>
internal sealed class BrokerClient : IAsyncDisposable
{
  /// <summary>
  ///   Name of the <see cref="HttpClient" /> registered for the broker.
  /// </summary>
  public const string HttpClientName = "ArmoniK.Broker";

  private static readonly JsonSerializerOptions Json = new()
                                                       {
                                                         PropertyNamingPolicy   = JsonNamingPolicy.SnakeCaseLower,
                                                         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                                                       };

  private readonly CancellationTokenSource            disposed_ = new();
  private readonly ConcurrentDictionary<string, byte> held_     = new();
  private readonly IHttpClientFactory                 httpClientFactory_;
  private readonly ILogger                            logger_;
  private readonly NodeBody?                          node_;
  private readonly Broker                             options_;
  private readonly Version                            version_;
  private          long                               epoch_ = -1;
  private          long                               renewPeriodMs_;
  private          int                                renewing_;

  public BrokerClient(Broker                options,
                      IHttpClientFactory    httpClientFactory,
                      ILogger<BrokerClient> logger)
  {
    options_           = options;
    httpClientFactory_ = httpClientFactory;
    logger_            = logger;
    version_ = options.Http2
                 ? HttpVersion.Version20
                 : HttpVersion.Version11;

    node_ = options.Affinity
              ? new NodeBody(string.IsNullOrEmpty(options.NodeId)
                               ? Environment.GetEnvironmentVariable("NODE_NAME") ?? Environment.MachineName
                               : options.NodeId,
                             options.CacheCapacityBytes)
              : null;
  }

  /// <summary>
  ///   Whether tasks are placed next to their data (<see cref="Broker.Affinity" />).
  /// </summary>
  public bool Affinity
    => options_.Affinity;

  /// <summary>
  ///   Largest enqueue batch; a batch the server finds too large is split (protocol §6.1).
  /// </summary>
  public int MaxBatchItems
    => Math.Max(1,
                options_.MaxBatchItems);

  public ValueTask DisposeAsync()
  {
    disposed_.Cancel();
    disposed_.Dispose();
    return ValueTask.CompletedTask;
  }

  /// <summary>
  ///   Posts a request and reads its answer, <c>null</c> when it has none. An error status throws an
  ///   <see cref="HttpRequestException" /> carrying it.
  /// </summary>
  /// <param name="path">Route, relative to the endpoint</param>
  /// <param name="body">Request body</param>
  /// <param name="once">Timeout of a request sent once; <c>null</c> to retry it (<see cref="RetryHandler" />)</param>
  /// <param name="cancellationToken">Token to cancel the request</param>
  private async Task<T?> PostAsync<T>(string            path,
                                      object            body,
                                      TimeSpan?         once,
                                      CancellationToken cancellationToken)
    where T : class
  {
    using var request = new HttpRequestMessage(HttpMethod.Post,
                                               path)
                        {
                          Version       = version_,
                          VersionPolicy = HttpVersionPolicy.RequestVersionExact,
                          Content = JsonContent.Create(body,
                                                       body.GetType(),
                                                       options: Json),
                        };
    // A linked source only for a request with its own timeout: the others are cancelled by the caller only.
    using var cts = once is null
                      ? null
                      : CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    if (once is { } timeout)
    {
      cts!.CancelAfter(timeout);
      request.Options.Set(RetryHandler.Disabled,
                          true);
    }

    var token = cts?.Token ?? cancellationToken;
    using var response = await httpClientFactory_.CreateClient(HttpClientName)
                                                 .SendAsync(request,
                                                            token)
                                                 .ConfigureAwait(false);
    ObserveEpoch(response);
    response.EnsureSuccessStatusCode();
    return response.StatusCode == HttpStatusCode.NoContent
             ? null
             : await response.Content.ReadFromJsonAsync<T>(Json,
                                                           token)
                             .ConfigureAwait(false);
  }

  private void ObserveEpoch(HttpResponseMessage response)
  {
    if (!response.Headers.TryGetValues("x-broker-epoch",
                                       out var values) || !long.TryParse(values.FirstOrDefault(),
                                                                         out var epoch))
    {
      return;
    }

    var previous = Interlocked.Exchange(ref epoch_,
                                        epoch);
    if (previous >= 0 && previous != epoch)
    {
      // The queue content was lost: tasks submitted before this point must be resumed (pause then resume their sessions).
      logger_.LogWarning("Broker restarted: epoch changed from {PreviousEpoch} to {Epoch}, its queued messages were lost",
                         previous,
                         epoch);
    }
  }

  public Task EnqueueAsync(string                     partition,
                           string                     key,
                           int                        priority,
                           IReadOnlyList<EnqueueItem> items,
                           CancellationToken          cancellationToken)
    => PostAsync<object>($"v1/partitions/{Uri.EscapeDataString(partition)}/messages",
                         new EnqueueBody(key,
                                         priority,
                                         items),
                         null,
                         cancellationToken);

  /// <summary>
  ///   Long polls messages. Never throws for a broker failure: it logs, waits and returns nothing,
  ///   so that a restart of the broker does not stop the Pollster.
  /// </summary>
  public async Task<IReadOnlyList<PulledMessage>> PullAsync(string            partition,
                                                            int               max,
                                                            CancellationToken cancellationToken)
  {
    try
    {
      var body = await PostAsync<PullResponse>($"v1/partitions/{Uri.EscapeDataString(partition)}/pull",
                                               new PullBody(Math.Max(1,
                                                                     max),
                                                            (long)options_.PullWait.TotalMilliseconds,
                                                            node_),
                                               options_.PullWait + options_.RequestTimeout,
                                               cancellationToken)
                   .ConfigureAwait(false);
      if (body is null)
      {
        return [];
      }

      foreach (var m in body.Messages)
      {
        held_[m.Token] = 0;
      }

      StartRenewing(body.LeaseMs);
      return body.Messages;
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      throw;
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Broker pull failed on partition {PartitionId}, retrying later",
                         partition);
      // Jittered, so that the agents do not all come back at once when the broker restarts.
      await Task.Delay(TimeSpan.FromSeconds(0.5 + Random.Shared.NextDouble()),
                       cancellationToken)
                .ConfigureAwait(false);
      return [];
    }
  }

  /// <summary>
  ///   Renews at a third of the lease: a renewal lost in a row still leaves time for the next one.
  ///   The loop starts with the first message received.
  /// </summary>
  private void StartRenewing(long leaseMs)
  {
    Volatile.Write(ref renewPeriodMs_,
                   Math.Max(1,
                            leaseMs / 3));
    if (Interlocked.Exchange(ref renewing_,
                             1) == 0)
    {
      _ = RenewLoopAsync(disposed_.Token);
    }
  }

  private async Task RenewLoopAsync(CancellationToken token)
  {
    while (!token.IsCancellationRequested)
    {
      try
      {
        var period = TimeSpan.FromMilliseconds(Volatile.Read(ref renewPeriodMs_));
        await Task.Delay(period,
                         token)
                  .ConfigureAwait(false);
        if (held_.IsEmpty)
        {
          continue;
        }

        var body = await PostAsync<RenewResponse>("v1/renew",
                                                  new RenewBody(held_.Select(kv => kv.Key)
                                                                .ToList()),
                                                  period,
                                                  token)
                     .ConfigureAwait(false);
        // Tokens that designate no current distribution any more: stop renewing them.
        foreach (var t in body?.Unknown ?? [])
        {
          if (held_.TryRemove(t,
                              out _))
          {
            logger_.LogWarning("Broker no longer holds message {MessageId}",
                               t);
          }
        }
      }
      catch (OperationCanceledException) when (token.IsCancellationRequested)
      {
        return;
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Broker lease renewal failed");
      }
    }
  }

  /// <summary>
  ///   Acknowledges a message. It stops being renewed first: if the settlement fails, the lease expires and
  ///   the broker redelivers the message instead of keeping it forever.
  /// </summary>
  public Task AckAsync(string       token,
                       OutputsBody? outputs)
  {
    held_.TryRemove(token,
                    out _);
    return PostAsync<object>("v1/ack",
                             new AckBody([
                                           new AckItem(token,
                                                       outputs),
                                         ]),
                             null,
                             disposed_.Token);
  }

  /// <summary>
  ///   Puts a message back in the queue at once; see <see cref="AckAsync" /> for the renewal.
  /// </summary>
  public Task NackAsync(string token)
  {
    held_.TryRemove(token,
                    out _);
    return PostAsync<object>("v1/nack",
                             new NackBody([new NackItem(token, "requeue")]),
                             null,
                             disposed_.Token);
  }

  // ------------------------------------------------------------------ wire types

  internal sealed record EnqueueItem(string        TaskId,
                                     AffinityBody? Affinity);

  internal sealed record AffinityBody(uint[] Hashes,
                                      int[]  Sizes,
                                      ushort DepCount,
                                      byte   TotalSize);

  internal sealed record OutputsBody(uint[] Hashes,
                                     int[]  Sizes);

  private sealed record EnqueueBody(string                     Key,
                                    int                        Priority,
                                    IReadOnlyList<EnqueueItem> Items);

  private sealed record NodeBody(string Id,
                                 long   CacheCapacityBytes);

  private sealed record PullBody(int       Max,
                                 long      WaitMs,
                                 NodeBody? Node);

  private sealed record PullResponse(long                         LeaseMs,
                                     IReadOnlyList<PulledMessage> Messages);

  private sealed record RenewBody(IReadOnlyList<string> Tokens);

  private sealed record RenewResponse(IReadOnlyList<string> Unknown);

  private sealed record AckItem(string       Token,
                                OutputsBody? Outputs);

  private sealed record AckBody(IReadOnlyList<AckItem> Items);

  private sealed record NackItem(string Token,
                                 string Policy);

  private sealed record NackBody(IReadOnlyList<NackItem> Items);
}
