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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Error returned by the broker, following protocol §4.
/// </summary>
public sealed class BrokerException : Exception
{
  internal BrokerException(HttpStatusCode status,
                              string         type,
                              bool           retryable,
                              string?        detail)
    : base($"Broker answered {(int)status} {type}: {detail}")
  {
    Status    = status;
    Type      = type;
    Retryable = retryable;
  }

  /// <summary>HTTP status</summary>
  public HttpStatusCode Status { get; }

  /// <summary>Problem type, without the URN prefix</summary>
  public string Type { get; }

  /// <summary>Whether the protocol allows a retry</summary>
  public bool Retryable { get; }
}

internal sealed record PulledMessage(string Token,
                                     string TaskId,
                                     int    Attempts);

/// <summary>
///   REST client of the broker (protocol v1). Retryable failures are absorbed with back-off and
///   jitter; consumers are registered lazily per partition and re-registered after a restart.
/// </summary>
internal sealed class BrokerClient : IAsyncDisposable
{
  private const string UrnPrefix = "urn:armonik:broker:";


  private static readonly JsonSerializerOptions Json = new()
                                                       {
                                                         PropertyNamingPolicy   = JsonNamingPolicy.SnakeCaseLower,
                                                         DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
                                                       };

  private readonly ConcurrentDictionary<string, Lazy<Task<Consumer>>> consumers_ = new();
  private readonly ConcurrentDictionary<string, Consumer>             byId_      = new();
  private readonly CancellationTokenSource                               disposed_  = new();
  private readonly HttpClient                                            http_;
  private readonly ILogger                                               logger_;
  private readonly Broker                                             options_;
  private readonly Version                                               version_;
  private          long                                                  epoch_ = -1;
  private          int                                                   limitsLoaded_;
  private          int                                                   maxBatchItems_ = int.MaxValue;
  private          int                                                   maxPull_       = Protocol.DefaultMaxPull;
  private          int                                                   refreshingLimits_;

  public BrokerClient(Broker options,
                         ILogger   logger)
  {
    options_ = options;
    logger_  = logger;
    version_ = options.Http2
                 ? HttpVersion.Version20
                 : HttpVersion.Version11;

    var handler = new SocketsHttpHandler
                  {
                    EnableMultipleHttp2Connections = true,
                    KeepAlivePingDelay             = options.KeepAlivePingPeriod,
                    KeepAlivePingTimeout           = options.KeepAlivePingPeriod,
                    KeepAlivePingPolicy            = HttpKeepAlivePingPolicy.Always,
                    ConnectTimeout                 = options.ConnectTimeout,
                    PooledConnectionLifetime       = Timeout.InfiniteTimeSpan,
                  };

    if (!string.IsNullOrEmpty(options.ClientCertificateFile))
    {
      handler.SslOptions.ClientCertificates = new X509CertificateCollection
                                              {
                                                X509Certificate2.CreateFromPemFile(options.ClientCertificateFile,
                                                                                   string.IsNullOrEmpty(options.ClientKeyFile)
                                                                                     ? null
                                                                                     : options.ClientKeyFile),
                                              };
    }

    if (!string.IsNullOrEmpty(options.CaFile))
    {
      handler.SslOptions.RemoteCertificateValidationCallback = CertificateValidator.CreateCallback(options.CaFile,
                                                                                                  options.AllowHostMismatch,
                                                                                                  logger);
    }

    http_ = new HttpClient(handler)
            {
              BaseAddress = new Uri(options.Endpoint.TrimEnd('/') + "/"),
              Timeout     = Timeout.InfiniteTimeSpan,
            };
  }

  public ValueTask DisposeAsync()
  {
    disposed_.Cancel();
    foreach (var c in consumers_.Values.Where(l => l.IsValueCreated && l.Value.IsCompletedSuccessfully))
    {
      c.Value.Result.Renewal.Dispose();
    }

    http_.Dispose();
    disposed_.Dispose();
    return ValueTask.CompletedTask;
  }

  private async Task<(HttpStatusCode Status, JsonElement? Body)> SendAsync(HttpMethod        method,
                                                                          string            path,
                                                                          object?           body,
                                                                          TimeSpan          timeout,
                                                                          CancellationToken cancellationToken)
  {
    using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    cts.CancelAfter(timeout);
    using var request = new HttpRequestMessage(method,
                                               path)
                        {
                          Version       = version_,
                          VersionPolicy = HttpVersionPolicy.RequestVersionExact,
                          Content = body is null
                                      ? null
                                      : JsonContent.Create(body,
                                                           body.GetType(),
                                                           options: Json),
                        };
    using var response = await http_.SendAsync(request,
                                               cts.Token)
                                    .ConfigureAwait(false);
    ObserveEpoch(response);

    JsonElement? content = null;
    var raw = await response.Content.ReadAsByteArrayAsync(cts.Token)
                            .ConfigureAwait(false);
    if (raw.Length > 0)
    {
      try
      {
        content = JsonSerializer.Deserialize<JsonElement>(raw);
      }
      catch (JsonException) when (!response.IsSuccessStatusCode)
      {
        // Non JSON error body (proxy, load balancer): the status alone is used.
      }
    }

    if (response.IsSuccessStatusCode)
    {
      return (response.StatusCode, content);
    }

    var type = content?.TryGetProperty("type",
                                       out var t) == true
                 ? t.GetString() ?? ""
                 : "";
    var retryable = content?.TryGetProperty("retryable",
                                            out var r) == true && r.GetBoolean();
    var detail = content?.TryGetProperty("detail",
                                         out var d) == true
                   ? d.GetString()
                   : response.ReasonPhrase;
    throw new BrokerException(response.StatusCode,
                                 type.StartsWith(UrnPrefix)
                                   ? type[UrnPrefix.Length..]
                                   : type,
                                 retryable || response.StatusCode is HttpStatusCode.TooManyRequests or HttpStatusCode.ServiceUnavailable,
                                 detail);
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
    // A restarted broker may run another configuration; limits never read are read now.
    if (previous != epoch && (previous >= 0 || Volatile.Read(ref limitsLoaded_) == 0))
    {
      _ = RefreshLimitsAsync(disposed_.Token);
    }

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
         BrokerException s          => s.Retryable,
         HttpRequestException          => true,
         TaskCanceledException         => true,
         System.IO.IOException         => true,
         _                             => false,
       };

  /// <summary>
  ///   Exponential back-off with ±50 % uniform jitter (protocol §5).
  /// </summary>
  private TimeSpan Backoff(int attempt)
  {
    var ms = Math.Min(options_.BackoffMax.TotalMilliseconds,
                      options_.BackoffMin.TotalMilliseconds * Math.Pow(2,
                                                                       Math.Min(attempt,
                                                                                Protocol.BackoffMaxDoublings)));
    return TimeSpan.FromMilliseconds(ms * (1 - Protocol.BackoffJitter + 2 * Protocol.BackoffJitter * Random.Shared.NextDouble()));
  }

  /// <summary>
  ///   Runs an operation, retrying retryable failures with back-off until <see cref="Broker.MaxRetryDuration" />.
  /// </summary>
  private async Task<T> RetryAsync<T>(Func<CancellationToken, Task<T>> operation,
                                      string                           what,
                                      CancellationToken                cancellationToken)
  {
    var watch = Stopwatch.StartNew();
    for (var attempt = 0;; attempt++)
    {
      try
      {
        return await operation(cancellationToken)
                 .ConfigureAwait(false);
      }
      catch (Exception e) when (!cancellationToken.IsCancellationRequested && IsRetryable(e) && watch.Elapsed < options_.MaxRetryDuration)
      {
        var delay = Backoff(attempt);
        logger_.LogWarning(e,
                           "Broker {Operation} failed, retrying in {Delay}",
                           what,
                           delay);
        await Task.Delay(delay,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }
  }

  // ------------------------------------------------------------------ producer

  public Task EnqueueAsync(string                         partition,
                           string                         key,
                           int                            priority,
                           IReadOnlyList<EnqueueItem>     items,
                           CancellationToken              cancellationToken)
    => RetryAsync(ct => SendAsync(HttpMethod.Post,
                                  $"v1/partitions/{Uri.EscapeDataString(partition)}/messages",
                                  new EnqueueBody(key,
                                                  priority,
                                                  items),
                                  options_.RequestTimeout,
                                  ct),
                  "enqueue",
                  cancellationToken);

  // ------------------------------------------------------------------ consumer

  private sealed class Consumer
  {
    public required string                  Id          { get; init; }
    public required CancellationTokenSource Renewal     { get; init; }
    public required TimeSpan                RenewPeriod { get; init; }

    /// <summary>
    ///   Tokens received and not settled yet. Only these are renewed: a message whose answer was lost,
    ///   or whose settlement was given up, is not renewed and the broker redelivers it after its lease.
    /// </summary>
    public ConcurrentDictionary<string, byte> Held { get; } = new();
  }

  private async Task<Consumer> RegisterAsync(string            partition,
                                             CancellationToken cancellationToken)
  {
    var nodeId = string.IsNullOrEmpty(options_.NodeId)
                   ? Environment.GetEnvironmentVariable("NODE_NAME") ?? Environment.MachineName
                   : options_.NodeId;
    var node = !options_.Affinity || nodeId == "-" || options_.CacheCapacityBytes <= 0
                 ? null
                 : new NodeBody(nodeId,
                                options_.CacheCapacityBytes);
    var (_, body) = await RetryAsync(ct => SendAsync(HttpMethod.Post,
                                                     "v1/consumers",
                                                     new RegisterBody(partition,
                                                                      node),
                                                     options_.RequestTimeout,
                                                     ct),
                                     "register",
                                     cancellationToken)
                      .ConfigureAwait(false);
    // Renew well within the server lease, whatever the configured period.
    var lease = TimeSpan.FromMilliseconds(body!.Value.GetProperty("lease_ms")
                                              .GetInt64());
    var consumer = new Consumer
                   {
                     Id      = body.Value.GetProperty("consumer_id").GetString()!,
                     Renewal = CancellationTokenSource.CreateLinkedTokenSource(disposed_.Token),
                     RenewPeriod = options_.RenewPeriod < lease / 3
                                     ? options_.RenewPeriod
                                     : lease / 3,
                   };
    byId_[consumer.Id] = consumer;
    logger_.LogInformation("Registered broker consumer {ConsumerId} on partition {PartitionId} (node {NodeId})",
                           consumer.Id,
                           partition,
                           node?.Id);
    _ = RenewLoopAsync(partition,
                       consumer);
    return consumer;
  }

  private async Task RenewLoopAsync(string   partition,
                                    Consumer consumer)
  {
    var token = consumer.Renewal.Token;
    while (!token.IsCancellationRequested)
    {
      try
      {
        await Task.Delay(consumer.RenewPeriod,
                         token)
                  .ConfigureAwait(false);
        var (_, body) = await SendAsync(HttpMethod.Post,
                                        $"v1/consumers/{consumer.Id}/renew",
                                        new RenewBody(consumer.Held.Keys.ToList()),
                                        options_.RenewTimeout,
                                        token)
                          .ConfigureAwait(false);
        // Tokens the broker no longer knows for this consumer are gone: stop renewing them.
        if (body?.TryGetProperty("unknown",
                                 out var unknown) == true)
        {
          foreach (var t in unknown.EnumerateArray())
          {
            if (consumer.Held.TryRemove(t.GetString()!,
                                        out _))
            {
              logger_.LogWarning("Broker no longer holds message {MessageId} for consumer {ConsumerId}",
                                 t.GetString(),
                                 consumer.Id);
            }
          }
        }
      }
      catch (OperationCanceledException) when (token.IsCancellationRequested)
      {
        return;
      }
      catch (BrokerException e) when (e.Status == HttpStatusCode.Gone)
      {
        Forget(partition,
               consumer);
        return;
      }
      catch (Exception e)
      {
        logger_.LogWarning(e,
                           "Broker lease renewal failed for {ConsumerId}",
                           consumer.Id);
      }
    }
  }

  private void Forget(string   partition,
                      Consumer consumer)
  {
    if (consumers_.TryGetValue(partition,
                               out var current) && current.IsValueCreated && current.Value.IsCompletedSuccessfully &&
        current.Value.Result == consumer)
    {
      consumers_.TryRemove(new KeyValuePair<string, Lazy<Task<Consumer>>>(partition,
                                                                          current));
    }

    byId_.TryRemove(consumer.Id,
                    out _);
    consumer.Renewal.Cancel();
  }

  private async Task<Consumer> GetConsumerAsync(string            partition,
                                                CancellationToken cancellationToken)
  {
    var lazy = consumers_.GetOrAdd(partition,
                                   p => new Lazy<Task<Consumer>>(() => RegisterAsync(p,
                                                                                     disposed_.Token)));
    try
    {
      return await lazy.Value.WaitAsync(cancellationToken)
                       .ConfigureAwait(false);
    }
    catch when (lazy.Value.IsFaulted)
    {
      consumers_.TryRemove(new KeyValuePair<string, Lazy<Task<Consumer>>>(partition,
                                                                          lazy));
      throw;
    }
  }

  /// <summary>
  ///   Long polls messages. Never throws for a broker failure: it logs, waits and returns nothing,
  ///   so that a restart of the broker does not stop the Pollster.
  /// </summary>
  public async Task<(string ConsumerId, IReadOnlyList<PulledMessage> Messages)> PullAsync(string            partition,
                                                                                         int               max,
                                                                                         CancellationToken cancellationToken)
  {
    Consumer? consumer = null;
    try
    {
      consumer = await GetConsumerAsync(partition,
                                        cancellationToken)
                   .ConfigureAwait(false);
      var (status, body) = await SendAsync(HttpMethod.Post,
                                           $"v1/consumers/{consumer.Id}/pull",
                                           new PullBody(Math.Clamp(max,
                                                                   1,
                                                                   Volatile.Read(ref maxPull_)),
                                                        (long)options_.PullWait.TotalMilliseconds),
                                           options_.PullWait + options_.PullTimeoutMargin,
                                           cancellationToken)
                             .ConfigureAwait(false);
      if (status == HttpStatusCode.NoContent || body is null)
      {
        return (consumer.Id, []);
      }

      var messages = body.Value.GetProperty("messages")
                         .EnumerateArray()
                         .Select(m => new PulledMessage(m.GetProperty("token").GetString()!,
                                                        m.GetProperty("task_id").GetString()!,
                                                        m.GetProperty("attempts").GetInt32()))
                         .ToList();
      foreach (var m in messages)
      {
        consumer.Held[m.Token] = 0;
      }

      return (consumer.Id, messages);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      throw;
    }
    catch (BrokerException e) when (e.Status == HttpStatusCode.Gone && consumer is not null)
    {
      logger_.LogInformation("Broker consumer {ConsumerId} is unknown, registering again",
                             consumer.Id);
      Forget(partition,
             consumer);
      return ("", []);
    }
    catch (Exception e)
    {
      logger_.LogWarning(e,
                         "Broker pull failed on partition {PartitionId}, retrying later",
                         partition);
      await Task.Delay(Backoff(3),
                       cancellationToken)
                .ConfigureAwait(false);
      return ("", []);
    }
  }

  /// <summary>
  ///   Stops renewing a message, before settling it: if the settlement fails, the lease expires and
  ///   the broker redelivers the message instead of keeping it forever.
  /// </summary>
  private void Release(string consumerId,
                       string token)
  {
    if (byId_.TryGetValue(consumerId,
                          out var consumer))
    {
      consumer.Held.TryRemove(token,
                              out _);
    }
  }

  public Task AckAsync(string       consumerId,
                       string       token,
                       OutputsBody? outputs)
  {
    Release(consumerId,
            token);
    if (!options_.Affinity)
    {
      outputs = null;
    }

    return RetryAsync(ct => SendAsync(HttpMethod.Post,
                                  $"v1/consumers/{consumerId}/ack",
                                  new AckBody([new AckItem(token, outputs)]),
                                  options_.RequestTimeout,
                                  ct),
                  "ack",
                  disposed_.Token);
  }

  public Task NackAsync(string consumerId,
                        string token)
  {
    Release(consumerId,
            token);
    return RetryAsync(ct => SendAsync(HttpMethod.Post,
                                  $"v1/consumers/{consumerId}/nack",
                                  new NackBody([new NackItem(token, "requeue")]),
                                  options_.RequestTimeout,
                                  ct),
                  "nack",
                  disposed_.Token);
  }

  /// <summary>
  ///   Largest enqueue batch: the configured size, bounded by the server limit.
  /// </summary>
  public int MaxBatchItems
    => Math.Max(1,
                Math.Min(options_.MaxBatchItems,
                         Volatile.Read(ref maxBatchItems_)));

  /// <summary>
  ///   Reads the limits of the server (<c>GET /v1/limits</c>). Returns false, keeping the current values,
  ///   when the server cannot be reached; they are read again at the next epoch change.
  /// </summary>
  public async Task<bool> RefreshLimitsAsync(CancellationToken cancellationToken)
  {
    if (Interlocked.Exchange(ref refreshingLimits_,
                             1) != 0)
    {
      return false;
    }

    try
    {
      var (_, body) = await SendAsync(HttpMethod.Get,
                                      "v1/limits",
                                      null,
                                      options_.ProbeTimeout,
                                      cancellationToken)
                        .ConfigureAwait(false);
      var limits = body!.Value;
      Volatile.Write(ref maxBatchItems_,
                     limits.GetProperty("max_batch_items")
                           .GetInt32());
      Volatile.Write(ref maxPull_,
                     limits.GetProperty("max_pull")
                           .GetInt32());
      Volatile.Write(ref limitsLoaded_,
                     1);
      logger_.LogDebug("Broker limits: {Limits}",
                       limits.ToString());
      return true;
    }
    catch (Exception e) when (e is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
    {
      logger_.LogWarning(e,
                         "Could not read the broker limits; defaults are used until it is reachable");
      return false;
    }
    finally
    {
      Volatile.Write(ref refreshingLimits_,
                     0);
    }
  }

  public async Task<bool> IsHealthyAsync(CancellationToken cancellationToken)
  {
    try
    {
      var (status, _) = await SendAsync(HttpMethod.Get,
                                        "v1/health",
                                        null,
                                        options_.ProbeTimeout,
                                        cancellationToken)
                          .ConfigureAwait(false);
      return status == HttpStatusCode.OK;
    }
    catch
    {
      return false;
    }
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

  private sealed record RegisterBody(string    Partition,
                                     NodeBody? Node);

  private sealed record RenewBody(IReadOnlyList<string> Tokens);

  private sealed record PullBody(int  Max,
                                 long WaitMs);

  private sealed record AckItem(string       Token,
                                OutputsBody? Outputs);

  private sealed record AckBody(IReadOnlyList<AckItem> Items);

  private sealed record NackItem(string Token,
                                 string Policy);

  private sealed record NackBody(IReadOnlyList<NackItem> Items);
}
