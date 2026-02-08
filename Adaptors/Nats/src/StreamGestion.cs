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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ArmoniK.Core.Adapters.Nats;

/// <summary>
///   Helper class to manage NATS JetStream streams and consumers with priority support.
/// </summary>
internal class StreamGestion
{
  private const string StreamName = "armonik-stream";
  private const string SubjectSeparator = ":";

  private readonly INatsJSContext js_;
  private readonly Nats           options_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="StreamGestion" /> class.
  /// </summary>
  /// <param name="js">The NATS JetStream context.</param>
  /// <param name="options">The NATS configuration options.</param>
  public StreamGestion(INatsJSContext js,
                       Nats           options)
  {
    js_      = js;
    options_ = options;
  }

  /// <summary>
  ///   Gets the maximum priority level. Minimum is 1.
  /// </summary>
  public int MaxPriority
    => int.Max(options_.MaxPriority,
               1);

  /// <summary>
  ///   Generates a subject name for a given partition and priority.
  /// </summary>
  /// <param name="partitionId">The partition identifier.</param>
  /// <param name="priority">The priority level (1-based).</param>
  /// <returns>The subject name (e.g., "partition:1").</returns>
  public static string GetSubjectName(string partitionId,
                                      int    priority)
    => partitionId + SubjectSeparator + priority;

  /// <summary>
  ///   Ensures the stream exists and all priority subjects for the partition are registered.
  /// </summary>
  /// <param name="partitionId">The partition identifier.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>The stream instance.</returns>
  public async Task<INatsJSStream> EnsureStreamExistsAsync(string            partitionId,
                                                            CancellationToken cancellationToken)
  {
    try
    {
      // Try to get existing stream
      var stream = await js_.GetStreamAsync(StreamName,
                                            cancellationToken: cancellationToken)
                            .ConfigureAwait(false);

      // Ensure all priority subjects are registered
      var updated = false;
      for (var priority = 1; priority <= MaxPriority; priority++)
      {
        var subject = GetSubjectName(partitionId,
                                     priority);
        if (!stream.Info.Config.Subjects!.Contains(subject))
        {
          stream.Info.Config.Subjects!.Add(subject);
          updated = true;
        }
      }

      if (updated)
      {
        stream = await js_.UpdateStreamAsync(stream.Info.Config,
                                             cancellationToken: cancellationToken)
                          .ConfigureAwait(false);
      }

      return stream;
    }
    catch (NatsJSApiException ex) when (ex.Error.Code == 404)
    {
      // Stream doesn't exist, try to create it
      try
      {
        var subjects = Enumerable.Range(1,
                                        MaxPriority)
                                 .Select(i => GetSubjectName(partitionId,
                                                             i))
                                 .ToArray();

        var config = new StreamConfig
                     {
                       Name      = StreamName,
                       Storage   = StreamConfigStorage.File,
                       Subjects  = subjects,
                       Retention = StreamConfigRetention.Workqueue,
                     };

        return await js_.CreateStreamAsync(config,
                                           cancellationToken)
                        .ConfigureAwait(false);
      }
      catch (NatsJSApiException ex2) when (ex2.Error.Code == 400)
      {
        // Race condition: another thread created the stream, fetch it
        return await js_.GetStreamAsync(StreamName,
                                        cancellationToken: cancellationToken)
                        .ConfigureAwait(false);
      }
    }
  }

  /// <summary>
  ///   Ensures consumers exist for all priority levels of a partition.
  /// </summary>
  /// <param name="partitionId">The partition identifier.</param>
  /// <param name="cancellationToken">Cancellation token.</param>
  /// <returns>Array of consumers, one per priority level (ordered by priority 1, 2, 3, ...).</returns>
  public async Task<INatsJSConsumer[]> EnsureConsumersExistAsync(string            partitionId,
                                                                  CancellationToken cancellationToken)
  {
    try
    {
      // Try to get existing consumers
      return await Task.WhenAll(Enumerable.Range(1,
                                                 MaxPriority)
                                          .Select(async priority =>
                                                  {
                                                    var consumerName = GetSubjectName(partitionId,
                                                                                      priority);
                                                    return await js_.GetConsumerAsync(StreamName,
                                                                                      consumerName,
                                                                                      cancellationToken)
                                                                    .ConfigureAwait(false);
                                                  }))
                   .ConfigureAwait(false);
    }
    catch (NatsJSApiException ex) when (ex.Error.Code == 404)
    {
      // One or more consumers don't exist, create them all
      return await Task.WhenAll(Enumerable.Range(1,
                                                 MaxPriority)
                                          .Select(async priority =>
                                                  {
                                                    var consumerName = GetSubjectName(partitionId,
                                                                                      priority);
                                                    var subject = GetSubjectName(partitionId,
                                                                                 priority);

                                                    return await js_.CreateConsumerAsync(StreamName,
                                                                                         new ConsumerConfig(consumerName)
                                                                                         {
                                                                                           DurableName   = consumerName,
                                                                                           AckWait       = TimeSpan.FromSeconds(options_.AckWait),
                                                                                           AckPolicy     = ConsumerConfigAckPolicy.Explicit,
                                                                                           FilterSubject = subject,
                                                                                         },
                                                                                         cancellationToken)
                                                                    .ConfigureAwait(false);
                                                  }))
                   .ConfigureAwait(false);
    }
  }
}
