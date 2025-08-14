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

using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ArmoniK.Core.Adapters.Nats;

internal class PushQueueStorage : IPushQueueStorage
{
  private readonly INatsJSContext js_;
  private readonly Nats           options_;
  private          bool           isInitialized_;

  public PushQueueStorage(INatsJSContext js,
                          Nats           options)
  {
    js_      = js;
    options_ = options;
  }

  public int MaxPriority
    => int.Max(options_.MaxPriority,
               1);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy("Plugin is not yet initialized."));

  /// <inheritdoc />
  /// <remarks>
  ///   only set is Initialized to true
  ///   <remarks />
  public async Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      var config = new StreamConfig
                   {
                     Name    = "armonik-stream",
                     Storage = StreamConfigStorage.File,
                   };
      await js_.CreateStreamAsync(config)
               .ConfigureAwait(false);
      isInitialized_ = true;
    }
  }

  /// <inheritdoc />
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
                               cancellationToken: cancellationToken)
                 .ConfigureAwait(false);
      }
    }
    catch (Exception)
    {
      var existing = await js_.GetStreamAsync("armonik-stream")
                              .ConfigureAwait(false);
      if (existing.Info.Config.Subjects != null)
      {
        existing.Info.Config.Subjects.Add(partitionId);
        await js_.UpdateStreamAsync(existing.Info.Config)
                 .ConfigureAwait(false);
      }
      else
      {
        var updated = existing.Info.Config with
                      {
                        Subjects = new[]
                                   {
                                     partitionId,
                                   },
                      };
        await js_.UpdateStreamAsync(updated)
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
