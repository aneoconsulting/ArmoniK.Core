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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Amqp;

public class QueueStorage : IQueueStorage
{
  private const   int             MaxInternalQueuePriority = 10;
  public readonly IConnectionAmqp ConnectionAmqp;

  public readonly int          NbLinks;
  public readonly Options.Amqp Options;

  public bool IsInitialized;

  public QueueStorage(Options.Amqp    options,
                      IConnectionAmqp connectionAmqp)
  {
    if (string.IsNullOrEmpty(options.Host))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Host)} is not defined.");
    }

    if (string.IsNullOrEmpty(options.User))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.User)} is not defined.");
    }

    if (string.IsNullOrEmpty(options.Password))
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Password)} is not defined.");
    }

    if (options.Port == 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.Port)} is not defined.");
    }

    if (options.MaxRetries == 0)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"{nameof(Options.MaxRetries)} is not defined.");
    }

    if (options.MaxPriority < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.MaxPriority)} is 1.");
    }

    if (options.LinkCredit < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(options),
                                            $"Minimum value for {nameof(Options.LinkCredit)} is 1.");
    }

    Options        = options;
    ConnectionAmqp = connectionAmqp;
    MaxPriority    = options.MaxPriority;
    PartitionId    = options.PartitionId;

    NbLinks = (MaxPriority + MaxInternalQueuePriority - 1) / MaxInternalQueuePriority;
  }

  /// <inheritdoc />
  public string PartitionId { get; }

  /// <inheritdoc />
  public virtual async Task Init(CancellationToken cancellationToken)
  {
    await ConnectionAmqp.Init(cancellationToken)
                        .ConfigureAwait(false);

    if (!IsInitialized)
    {
      IsInitialized = true;
    }
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (!IsInitialized)
    {
      return Task.FromResult(tag != HealthCheckTag.Liveness
                               ? HealthCheckResult.Degraded($"{nameof(QueueStorage)} is not yet initialized")
                               : HealthCheckResult.Unhealthy($"{nameof(QueueStorage)} is not yet initialized"));
    }

    return Task.FromResult(HealthCheckResult.Healthy());
  }

  /// <inheritdoc />
  public int MaxPriority { get; }
}
