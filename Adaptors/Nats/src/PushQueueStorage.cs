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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Utils;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ArmoniK.Core.Adapters.Nats;

/// <summary>
///   Insert messages into the queue.
/// </summary>
internal class PushQueueStorage : IPushQueueStorage
{
  private readonly INatsJSContext js_;
  private readonly Nats           options_;
  private          bool           isInitialized_;

  /// <summary>
  ///   Set Jet Stream and option as member value
  ///   if the configured value is less than 1, it defaults to 1.
  /// </summary>
  /// <param name="js">The DI container to register services into.</param>
  /// <param name="options">The options </param>
  public PushQueueStorage(INatsJSContext js,
                          Nats           options)
  {
    js_      = js;
    options_ = options;
  }


  /// <inheritdoc />
  public int MaxPriority
    => int.MaxValue;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  /// <remarks>
  ///   Attempts to publish all messages to the given subject.
  ///   If publishing fails, the method checks whether the stream exists:
  ///   - If the stream exists, it updates the stream configuration to include the subject.
  ///   - If the stream does not exist (404), it creates a new stream with the subject.
  ///   After fixing the stream, the messages are published again.
  /// </remarks>
  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = default)
  {
    try
    {
      await Publish(messages,
                    partitionId,
                    cancellationToken);
    }
    catch (Exception)
    {
      try
      {
        var existing = await js_.GetStreamAsync("armonik-stream")
                                .ConfigureAwait(false);
        if (!existing.Info.Config.Subjects!.Contains(partitionId))
        {
          existing.Info.Config.Subjects!.Add(partitionId);
          await js_.UpdateStreamAsync(existing.Info.Config)
                   .ConfigureAwait(false);
        }
      }
      catch (NatsJSApiException ex) when (ex.Error.Code == 404)
      {
        var config = new StreamConfig
                     {
                       Name    = "armonik-stream",
                       Storage = StreamConfigStorage.File,
                       Subjects = new[]
                                  {
                                    partitionId,
                                  },
                       Retention = StreamConfigRetention.Workqueue,
                     };
        await js_.CreateStreamAsync(config)
                 .ConfigureAwait(false);
      }

      await Publish(messages,
                    partitionId,
                    cancellationToken);
    }
  }

  /// <summary>
  ///   Push all messages to partitionId subject in queue.
  /// </summary>
  /// <param name="messages">an IEnumerable containing taskId.</param>
  /// <param name="partitionId">The partition Id use for this tasks.</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns> Task representing the asynchronous execution of the method.</returns>
  private async Task Publish(IEnumerable<MessageData> messages,
                             string                   partitionId,
                             CancellationToken        cancellationToken = default)
    => await messages.ParallelForEach(new ParallelTaskOptions(options_.DegreeOfParallelism,
                                                              cancellationToken),
                                      async message =>
                                      {
                                        await js_.PublishAsync(partitionId,
                                                               Encoding.UTF8.GetBytes(message.TaskId),
                                                               headers: new NatsHeaders
                                                                        {
                                                                          {
                                                                            "Nats-Msg-Id", Guid.NewGuid()
                                                                                               .ToString()
                                                                          },
                                                                        },
                                                               cancellationToken: cancellationToken)
                                                 .ConfigureAwait(false);
                                      })
                     .ConfigureAwait(false);
}
