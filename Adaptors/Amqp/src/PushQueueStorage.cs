// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Api.Worker.Utils;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PushQueueStorage : IPushQueueStorage
{
  private const int MaxInternalQueuePriority = 10;

  private readonly ILogger<PushQueueStorage>? logger_;

  private readonly int           nbLinks_;
  private readonly ISessionAmqp? sessionAmqp_;

  public bool IsInitialized;

  private SenderLink? sender_;

  public PushQueueStorage(Options.Amqp          options,
                          ISessionAmqp          sessionAmqp,
                          ILogger<PushQueueStorage> logger)
  {
    if (string.IsNullOrEmpty(options.Host))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.Host)} is not defined.");
    }

    if (string.IsNullOrEmpty(options.User))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.User)} is not defined.");
    }

    if (string.IsNullOrEmpty(options.Password))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.Password)} is not defined.");
    }

    if (options.Port == 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.Port)} is not defined.");
    }

    if (options.MaxRetries == 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Amqp.MaxRetries)} is not defined.");
    }

    if (options.MaxPriority < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.Amqp.MaxPriority)} is 1.");
    }

    if (options.LinkCredit < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.Amqp.LinkCredit)} is 1.");
    }

    sessionAmqp_ = sessionAmqp;
    MaxPriority  = options.MaxPriority;
    PartitionId  = options.PartitionId;
    logger_      = logger;

    nbLinks_ = (MaxPriority + MaxInternalQueuePriority - 1) / MaxInternalQueuePriority;
  }

  public  Task Init(CancellationToken cancellationToken)
  {
    if (!IsInitialized)
    {
      IsInitialized = true;
    }

    return Task.CompletedTask;
  }


  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(IsInitialized);

  /// <inheritdoc />
  public int MaxPriority { get; }

  /// <inheritdoc />
  public string PartitionId { get; }

  /// <inheritdoc />
  public async Task PushMessagesAsync(IEnumerable<string> messages,
                                      string              partitionId,
                                      int                 priority          = 1,
                                      CancellationToken   cancellationToken = default)
  {
    using var _ = logger_!.LogFunction();

    var whichQueue = priority < MaxInternalQueuePriority
                       ? priority / MaxInternalQueuePriority
                       : nbLinks_ - 1;
    var internalPriority = priority < MaxInternalQueuePriority
                             ? priority % MaxInternalQueuePriority
                             : MaxInternalQueuePriority;

    logger_!.LogDebug("Priority is {priority} ; will use queue #{queueId} with internal priority {internal priority}",
                      priority,
                      whichQueue,
                      internalPriority);

    sender_ = new SenderLink(sessionAmqp_!.Session,
                             $"{partitionId}###SenderLink{whichQueue}",
                             $"{partitionId}###q{whichQueue}");

    await Task.WhenAll(messages.Select(id => sender_.SendAsync(new Message(Encoding.UTF8.GetBytes(id))
                                                               {
                                                                 Header = new Header
                                                                          {
                                                                            Priority = (byte)internalPriority,
                                                                          },
                                                                 Properties = new Properties(),
                                                               })))
              .ConfigureAwait(false);

    await sender_.CloseAsync()
                 .ConfigureAwait(false);
  }
}
