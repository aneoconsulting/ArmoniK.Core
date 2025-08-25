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

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NATS.Client;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ArmoniK.Core.Adapters.Nats;

/// <inheritdoc />
internal class PushQueueStorage : IPushQueueStorage
{
  private readonly INatsJSContext js_;
  private readonly Nats           options_;
  private          bool           isInitialized_;

  /// <summary>
  /// Set Jet Stream and option as member value
  /// if the configured value is less than 1, it defaults to 1.
  /// </summary>
  /// <param name="js">The DI container to register services into.</param>
  /// <param name="options">The options </param>
  public PushQueueStorage(INatsJSContext js,
                          Nats           options)
  {
    js_      = js;
    options_ = options;
  }


  /// <summary>
  /// Gets the maximum priority. Ensures the value is at least 1;
  /// if the configured value is less than 1, it defaults to 1.
  /// </summary>
  public int MaxPriority
    => int.Max(options_.MaxPriority,
               1);

  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  /// <inheritdoc />
  /// <remarks>
  /// Set init to true.
  /// <remarks />
  public Task Init(CancellationToken cancellationToken)
  {
      isInitialized_ = true;
    return Task.CompletedTask;
  }

  /// <inheritdoc />
  /// <remarks>
  /// Attempts to publish all messages to the given subject.  
  /// If publishing fails, the method checks whether the stream exists:  
  /// - If the stream exists, it updates the stream configuration to include the subject.  
  /// - If the stream does not exist (404), it creates a new stream with the subject.  
  /// After fixing the stream, the messages are published again.
  /// <remarks />
  public async Task PushMessagesAsync(IEnumerable<MessageData> messages,
                                      string                   partitionId,
                                      CancellationToken        cancellationToken = default)
  {
    try
    {
      foreach (var message in messages)
      {
        await js_.PublishAsync(partitionId,
                               Encoding.UTF8.GetBytes(message.TaskId),
                               opts :new NatsJSPubOpts() {MsgId = Guid.NewGuid().ToString() },
                               cancellationToken: cancellationToken)
                 .ConfigureAwait(false);
      }
    }
    catch (Exception)
    {
      try
      {
        var existing = await js_.GetStreamAsync("armonik-stream")
                                .ConfigureAwait(false);
        existing.Info.Config.Subjects!.Add(partitionId);
        await js_.UpdateStreamAsync(existing.Info.Config)
                  .ConfigureAwait(false);
      } catch (NatsJSApiException ex) when (ex.Error.Code == 404)
      {
        var config = new StreamConfig
        {
          Name = "armonik-stream",
          Storage = StreamConfigStorage.File,
          Subjects = new[]
                             {
                                     partitionId,
                                   },
        };
        await js_.CreateOrUpdateStreamAsync(config)
                 .ConfigureAwait(false);
      }

      foreach (var message in messages)
      {
        await js_.PublishAsync(partitionId,
                               Encoding.UTF8.GetBytes(message.TaskId),
                               cancellationToken: cancellationToken)
                 .ConfigureAwait(false);
      }
    }
  }
}
