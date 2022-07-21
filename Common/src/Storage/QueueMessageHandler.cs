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
// but WITHOUT ANY WARRANTY

using System;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Worker.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Storage;

public class QueueMessageHandler : IQueueMessageHandler
{
  private readonly Func<QueueMessageStatus, Task> disposeFunc_;
  private readonly ILogger                        logger_;

  public QueueMessageHandler(string                         messageId,
                             string                         taskId,
                             Func<QueueMessageStatus, Task> disposeFunc,
                             ILogger                        logger,
                             CancellationToken              cancellationToken)
  {
    disposeFunc_      = disposeFunc;
    logger_           = logger;
    MessageId         = messageId;
    TaskId            = taskId;
    CancellationToken = cancellationToken;
  }

  public string MessageId { get; init; }
  public string TaskId    { get; init; }

  /// <inheritdoc />
  public QueueMessageStatus Status { get; set; }

  public CancellationToken CancellationToken { get; init; }

  /// <inheritdoc />
  public async ValueTask DisposeAsync()
  {
    using var _ = logger_.LogFunction(MessageId,
                                      functionName: $"{nameof(QueueMessageHandler)}.{nameof(DisposeAsync)}");
    await disposeFunc_(Status)
      .ConfigureAwait(false);
    GC.SuppressFinalize(this);
  }
}
