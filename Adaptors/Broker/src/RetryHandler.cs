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
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Polly;
using Polly.Retry;
using Polly.Timeout;

namespace ArmoniK.Core.Adapters.Broker;

/// <summary>
///   Retries the requests to the broker that fail transiently (protocol §4 and §5): 429 and 503, waiting what
///   <c>Retry-After</c> asks, network failures and timeouts, until <see cref="Broker.MaxRetryDuration" />.
///   Each attempt is bounded by <see cref="Broker.RequestTimeout" />.
/// </summary>
internal sealed class RetryHandler : DelegatingHandler
{
  /// <summary>
  ///   Set on a request that must be sent once, such as a pull or a renewal.
  /// </summary>
  public static readonly HttpRequestOptionsKey<bool> Disabled = new("ArmoniK.Broker.NoRetry");

  private readonly ResiliencePipeline<HttpResponseMessage> pipeline_;

  public RetryHandler(Broker                options,
                      ILogger<RetryHandler> logger)
    => pipeline_ = new ResiliencePipelineBuilder<HttpResponseMessage>().AddTimeout(options.MaxRetryDuration)
                                                                       .AddRetry(new RetryStrategyOptions<HttpResponseMessage>
                                                                                 {
                                                                                   ShouldHandle = new PredicateBuilder<HttpResponseMessage>()
                                                                                                  .HandleResult(r => r.StatusCode is HttpStatusCode.TooManyRequests
                                                                                                                                     or HttpStatusCode.ServiceUnavailable)
                                                                                                  .Handle<HttpRequestException>()
                                                                                                  .Handle<TimeoutRejectedException>(),
                                                                                   MaxRetryAttempts = int.MaxValue,
                                                                                   BackoffType      = DelayBackoffType.Exponential,
                                                                                   UseJitter        = true,
                                                                                   Delay            = Protocol.BackoffMin,
                                                                                   MaxDelay         = Protocol.BackoffMax,
                                                                                   DelayGenerator = args => ValueTask.FromResult(args.Outcome.Result?.Headers.RetryAfter switch
                                                                                                                                 {
                                                                                                                                   { Delta: { } delta } => delta,
                                                                                                                                   { Date: { } date } => date -
                                                                                                                                                         DateTimeOffset.UtcNow,
                                                                                                                                   _ => (TimeSpan?)null,
                                                                                                                                 }),
                                                                                   OnRetry = args =>
                                                                                             {
                                                                                               logger.LogWarning(args.Outcome.Exception,
                                                                                                                 "Broker answered {Status}, retrying in {Delay}",
                                                                                                                 args.Outcome.Result?.StatusCode,
                                                                                                                 args.RetryDelay);
                                                                                               args.Outcome.Result?.Dispose();
                                                                                               return ValueTask.CompletedTask;
                                                                                             },
                                                                                 })
                                                                       .AddTimeout(options.RequestTimeout)
                                                                       .Build();

  protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                                                               CancellationToken  cancellationToken)
  {
    if (request.Options.TryGetValue(Disabled,
                                    out var disabled) && disabled)
    {
      return await base.SendAsync(request,
                                  cancellationToken)
                       .ConfigureAwait(false);
    }

    return await pipeline_.ExecuteAsync(async ct => await base.SendAsync(request,
                                                                         ct)
                                                              .ConfigureAwait(false),
                                        cancellationToken)
                          .ConfigureAwait(false);
  }
}
