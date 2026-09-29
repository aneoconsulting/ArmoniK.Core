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

using Polly;
using Polly.Retry;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Error returned by the broker, following protocol §4.
/// </summary>
public sealed class BrokerException : Exception
{
  internal BrokerException(HttpStatusCode status,
                           string         type,
                           TimeSpan?      retryAfter,
                           string?        detail)
    : base($"Broker answered {(int)status} {type}: {detail}")
  {
    Status     = status;
    Type       = type;
    RetryAfter = retryAfter;
  }

  /// <summary>HTTP status</summary>
  public HttpStatusCode Status { get; }

  /// <summary>Problem type, without the URN prefix</summary>
  public string Type { get; }

  /// <summary>Whether the protocol allows a retry: only 429 and 503 do (§4)</summary>
  public bool Retryable
    => Status is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable;

  /// <summary>Delay the server asked for before a retry (<c>Retry-After</c>)</summary>
  public TimeSpan? RetryAfter { get; }
}

internal sealed record PulledMessage(string Token,
                                     string TaskId,
                                     int    Attempts);

/// <summary>
///   REST client of the broker (protocol v1). Stateless towards the server: it holds the tokens it
///   received and renews them all in one call. Retryable failures are absorbed with back-off and jitter.
/// </summary>
internal sealed class BrokerClient : IAsyncDisposable
{
  /// <summary>
  ///   Name of the <see cref="HttpClient" /> registered for the broker.
  /// </summary>
  public const string HttpClientName = "ArmoniK.Broker";

  private const string UrnPrefix = "urn:armonik:broker:";

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
  private readonly ResiliencePipeline                 retry_;
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

    var nodeId = string.IsNullOrEmpty(options.NodeId)
                   ? Environment.GetEnvironmentVariable("NODE_NAME") ?? Environment.MachineName
                   : options.NodeId;
    node_ = !options.Affinity || nodeId == "-" || options.CacheCapacityBytes <= 0
              ? null
              : new NodeBody(nodeId,
                             options.CacheCapacityBytes);

    // Retryable failures are retried until MaxRetryDuration, waiting what the server asks (Retry-After) when it does (protocol §5).
    retry_ = new ResiliencePipelineBuilder().AddTimeout(options.MaxRetryDuration)
                                            .AddRetry(new RetryStrategyOptions
                                                      {
                                                        ShouldHandle = args => ValueTask.FromResult(!args.Context.CancellationToken.IsCancellationRequested &&
                                                                                                    args.Outcome.Exception is { } e && IsRetryable(e)),
                                                        MaxRetryAttempts = int.MaxValue,
                                                        BackoffType      = DelayBackoffType.Exponential,
                                                        UseJitter        = true,
                                                        Delay            = Protocol.BackoffMin,
                                                        MaxDelay         = Protocol.BackoffMax,
                                                        DelayGenerator = args => ValueTask.FromResult(args.Outcome.Exception is BrokerException
                                                                                                                                  {
                                                                                                                                    RetryAfter: { } after,
                                                                                                                                  }
                                                                                                        ? after
                                                                                                        : (TimeSpan?)null),
                                                        OnRetry = args =>
                                                                  {
                                                                    logger_.LogWarning(args.Outcome.Exception,
                                                                                       "Broker request failed, retrying in {Delay}",
                                                                                       args.RetryDelay);
                                                                    return ValueTask.CompletedTask;
                                                                  },
                                                      })
                                            .Build();
  }

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

  private async Task<(HttpStatusCode Status, T? Body)> SendAsync<T>(HttpMethod        method,
                                                                    string            path,
                                                                    object            body,
                                                                    TimeSpan          timeout,
                                                                    CancellationToken cancellationToken)
    where T : class
  {
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    cts.CancelAfter(timeout);
    using var request = new HttpRequestMessage(method,
                                               path)
                        {
                          Version       = version_,
                          VersionPolicy = HttpVersionPolicy.RequestVersionExact,
                          Content = JsonContent.Create(body,
                                                       body.GetType(),
                                                       options: Json),
                        };
    using var response = await httpClientFactory_.CreateClient(HttpClientName)
                                                 .SendAsync(request,
                                                            cts.Token)
                                                 .ConfigureAwait(false);
    ObserveEpoch(response);
    var raw = await response.Content.ReadAsByteArrayAsync(cts.Token)
                            .ConfigureAwait(false);

    if (response.IsSuccessStatusCode)
    {
      return (response.StatusCode, raw.Length > 0
                                     ? JsonSerializer.Deserialize<T>(raw,
                                                                     Json)
                                     : null);
    }

    ProblemBody? problem = null;
    try
    {
      problem = raw.Length > 0
                  ? JsonSerializer.Deserialize<ProblemBody>(raw,
                                                            Json)
                  : null;
    }
    catch (JsonException)
    {
      // Non JSON error body (proxy, load balancer): the status alone is used.
    }

    var type = problem?.Type ?? "";
    throw new BrokerException(response.StatusCode,
                              type.StartsWith(UrnPrefix,
                                              StringComparison.Ordinal)
                                ? type[UrnPrefix.Length..]
                                : type,
                              response.Headers.RetryAfter switch
                              {
                                { Delta: { } delta } => delta,
                                { Date: { } date }   => date - DateTimeOffset.UtcNow,
                                _                    => null,
                              },
                              problem?.Detail ?? response.ReasonPhrase);
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

  private static bool IsRetryable(Exception e)
    => e switch
       {
         BrokerException s     => s.Retryable,
         HttpRequestException  => true,
         TaskCanceledException => true,
         System.IO.IOException => true,
         _                     => false,
       };

  /// <summary>
  ///   Runs an operation, retrying retryable failures with back-off until <see cref="Broker.MaxRetryDuration" />.
  /// </summary>
  private async Task RetryAsync(Func<CancellationToken, Task> operation,
                                CancellationToken             cancellationToken)
    => await retry_.ExecuteAsync(async ct => await operation(ct)
                                               .ConfigureAwait(false),
                                 cancellationToken)
                   .ConfigureAwait(false);

  public Task EnqueueAsync(string                     partition,
                           string                     key,
                           int                        priority,
                           IReadOnlyList<EnqueueItem> items,
                           CancellationToken          cancellationToken)
    => RetryAsync(ct => SendAsync<EnqueueResponse>(HttpMethod.Post,
                                                   $"v1/partitions/{Uri.EscapeDataString(partition)}/messages",
                                                   new EnqueueBody(key,
                                                                   priority,
                                                                   items),
                                                   options_.RequestTimeout,
                                                   ct),
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
      var (_, body) = await SendAsync<PullResponse>(HttpMethod.Post,
                                                    $"v1/partitions/{Uri.EscapeDataString(partition)}/pull",
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
      await Task.Delay((e as BrokerException)?.RetryAfter ?? TimeSpan.FromSeconds(0.5 + Random.Shared.NextDouble()),
                       cancellationToken)
                .ConfigureAwait(false);
      return [];
    }
  }

  /// <summary>
  ///   Renews well within the server lease: a renewal lost in a row still leaves time for the next one.
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

        var (_, body) = await SendAsync<RenewResponse>(HttpMethod.Post,
                                                       "v1/renew",
                                                       new RenewBody(held_.Keys.ToList()),
                                                       period,
                                                       token)
                          .ConfigureAwait(false);
        if (body is null)
        {
          continue;
        }

        Volatile.Write(ref renewPeriodMs_,
                       Math.Max(1,
                                body.LeaseMs / 3));
        // Tokens that designate no current distribution any more: stop renewing them.
        foreach (var t in body.Unknown)
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
    return RetryAsync(ct => SendAsync<SettleResponse>(HttpMethod.Post,
                                                      "v1/ack",
                                                      new AckBody([
                                                                    new AckItem(token,
                                                                                options_.Affinity
                                                                                  ? outputs
                                                                                  : null),
                                                                  ]),
                                                      options_.RequestTimeout,
                                                      ct),
                      disposed_.Token);
  }

  /// <summary>
  ///   Puts a message back in the queue at once; see <see cref="AckAsync" /> for the renewal.
  /// </summary>
  public Task NackAsync(string token)
  {
    held_.TryRemove(token,
                    out _);
    return RetryAsync(ct => SendAsync<SettleResponse>(HttpMethod.Post,
                                                      "v1/nack",
                                                      new NackBody([new NackItem(token, "requeue")]),
                                                      options_.RequestTimeout,
                                                      ct),
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

  private sealed record EnqueueResponse(int Accepted);

  private sealed record NodeBody(string Id,
                                 long   CacheCapacityBytes);

  private sealed record PullBody(int       Max,
                                 long      WaitMs,
                                 NodeBody? Node);

  private sealed record PullResponse(long                         LeaseMs,
                                     IReadOnlyList<PulledMessage> Messages);

  private sealed record RenewBody(IReadOnlyList<string> Tokens);

  private sealed record RenewResponse(long                  LeaseMs,
                                      IReadOnlyList<string> Unknown);

  private sealed record AckItem(string       Token,
                                OutputsBody? Outputs);

  private sealed record AckBody(IReadOnlyList<AckItem> Items);

  private sealed record NackItem(string Token,
                                 string Policy);

  private sealed record NackBody(IReadOnlyList<NackItem> Items);

  private sealed record SettleResponse(int Applied,
                                       int Ignored);

  private sealed record ProblemBody(string? Type,
                                    string? Detail);
}
