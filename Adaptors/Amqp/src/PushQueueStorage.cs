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

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Amqp;
using Amqp.Framing;

using ArmoniK.Api.Worker.Utils;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Amqp;

public class PushQueueStorage : QueueStorage, IPushQueueStorage
{
  private const int MaxInternalQueuePriority = 10;

  private readonly ILogger<PushQueueStorage>? logger_;

  public PushQueueStorage(Options.Amqp              options,
                          ISessionAmqp              sessionAmqp,
                          ILogger<PushQueueStorage> logger)
    : base(options,
           sessionAmqp)
    => logger_ = logger;

  /// <inheritdoc />
  public async Task PushMessagesAsync(IEnumerable<string> messages,
                                      string              partitionId,
                                      int                 priority          = 1,
                                      CancellationToken   cancellationToken = default)
  {
    using var _ = logger_!.LogFunction();

    var whichQueue = priority < MaxInternalQueuePriority
                       ? priority / MaxInternalQueuePriority
                       : NbLinks - 1;
    var internalPriority = priority < MaxInternalQueuePriority
                             ? priority % MaxInternalQueuePriority
                             : MaxInternalQueuePriority;

    logger_!.LogDebug("Priority is {priority} ; will use queue #{queueId} with internal priority {internal priority}",
                      priority,
                      whichQueue,
                      internalPriority);

    var sender = new SenderLink(SessionAmqp!.Session,
                                $"{partitionId}###SenderLink{whichQueue}",
                                $"{partitionId}###q{whichQueue}");

    await Task.WhenAll(messages.Select(id => sender.SendAsync(new Message(Encoding.UTF8.GetBytes(id))
                                                              {
                                                                Header = new Header
                                                                         {
                                                                           Priority = (byte)internalPriority,
                                                                         },
                                                                Properties = new Properties(),
                                                              })))
              .ConfigureAwait(false);

    await sender.CloseAsync()
                .ConfigureAwait(false);
  }
}
