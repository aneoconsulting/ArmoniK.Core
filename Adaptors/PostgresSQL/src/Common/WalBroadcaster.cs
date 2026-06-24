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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Holds a single shared WAL replication connection for one event type and fans out decoded
///   events to all active subscribers via in-memory channels. The replication connection is
///   opened on the first subscriber and closed when the last subscriber disconnects.
/// </summary>
/// <typeparam name="T">The decoded event payload type (e.g. TaskData, Result).</typeparam>
internal sealed class WalBroadcaster<T> : IDisposable
  where T : class
{
  private readonly NpgsqlConnectionProvider                                              provider_;
  private readonly Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse_;

  private readonly SemaphoreSlim                         lock_        = new(1, 1);
  private readonly ConcurrentDictionary<Guid, Channel<T>> subscribers_ = new();
  private          CancellationTokenSource?              loopCts_;
  private          Task?                                 broadcastLoop_;

  internal WalBroadcaster(NpgsqlConnectionProvider                                              provider,
                           Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse)
  {
    provider_ = provider;
    tryParse_ = tryParse;
  }

  /// <summary>
  ///   Subscribes to the broadcast stream. Starts the shared WAL loop if it is not running.
  ///   The loop is stopped when the last subscriber unsubscribes.
  /// </summary>
  public async IAsyncEnumerable<T> Subscribe([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var id      = Guid.NewGuid();
    var channel = Channel.CreateUnbounded<T>();
    subscribers_[id] = channel;

    await EnsureStarted(cancellationToken)
      .ConfigureAwait(false);

    try
    {
      await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken)
                                        .ConfigureAwait(false))
        yield return item;
    }
    finally
    {
      subscribers_.TryRemove(id,
                             out _);
      channel.Writer.TryComplete();
      await StopIfIdle()
        .ConfigureAwait(false);
    }
  }

  private async Task EnsureStarted(CancellationToken cancellationToken)
  {
    await lock_.WaitAsync(cancellationToken)
               .ConfigureAwait(false);
    try
    {
      if (broadcastLoop_ is null || broadcastLoop_.IsCompleted)
      {
        loopCts_       = new CancellationTokenSource();
        broadcastLoop_ = RunLoop(loopCts_.Token);
      }
    }
    finally
    {
      lock_.Release();
    }
  }

  private async Task StopIfIdle()
  {
    await lock_.WaitAsync()
               .ConfigureAwait(false);
    try
    {
      if (subscribers_.IsEmpty)
        loopCts_?.Cancel();
    }
    finally
    {
      lock_.Release();
    }
  }

  private async Task RunLoop(CancellationToken cancellationToken)
  {
    Exception? loopException = null;
    try
    {
      var slotName = $"armonik_{Guid.NewGuid():N}";
      var options  = new PgOutputReplicationOptions("armonik_pub", PgOutputProtocolVersion.V1, binary: true);

      await using var replConn = provider_.CreateReplicationConnection();
      await replConn.Open(cancellationToken)
                    .ConfigureAwait(false);

      var slot = await replConn.CreatePgOutputReplicationSlot(slotName,
                                                              temporarySlot: true,
                                                              cancellationToken: cancellationToken)
                               .ConfigureAwait(false);

      await foreach (var message in replConn.StartReplication(slot,
                                                              options,
                                                              cancellationToken)
                                            .ConfigureAwait(false))
      {
        replConn.SetReplicationStatus(message.WalEnd);

        var item = await tryParse_(message,
                                   cancellationToken)
                     .ConfigureAwait(false);
        if (item is not null)
        {
          foreach (var (_, ch) in subscribers_)
            ch.Writer.TryWrite(item);
        }
      }
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
      // Normal shutdown — channels are completed cleanly in the finally block
    }
    catch (Exception ex)
    {
      loopException = ex;
    }
    finally
    {
      foreach (var (_, ch) in subscribers_)
        ch.Writer.TryComplete(loopException);
    }
  }

  public void Dispose()
  {
    loopCts_?.Cancel();
    loopCts_?.Dispose();
    lock_.Dispose();
  }
}
