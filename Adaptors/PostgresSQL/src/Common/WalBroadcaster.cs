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
///   events to all active subscribers via in-memory channels.
///   <para>
///     The replication connection and slot are opened when the first subscriber registers.
///     The slot creation is signalled via an internal <see cref="TaskCompletionSource" /> so
///     that callers can wait until the slot exists before performing an initial DB snapshot —
///     this eliminates the race window where events between snapshot and slot creation would
///     otherwise be lost.
///   </para>
///   <para>
///     The connection is closed when the last subscriber disconnects.
///   </para>
/// </summary>
/// <typeparam name="T">The decoded event payload type (e.g. TaskData, Result).</typeparam>
internal sealed class WalBroadcaster<T> : IDisposable
  where T : class
{
  private readonly NpgsqlConnectionProvider                                       provider_;
  private readonly Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse_;

  private readonly SemaphoreSlim                           lock_        = new(1, 1);
  private          CancellationTokenSource?                loopCts_;
  private readonly ConcurrentDictionary<Guid, Channel<T>> subscribers_ = new();
  private          Task?                                  broadcastLoop_;
  private          TaskCompletionSource?                  slotReady_;

  internal WalBroadcaster(NpgsqlConnectionProvider                                       provider,
                           Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse)
  {
    provider_ = provider;
    tryParse_ = tryParse;
  }

  /// <summary>
  ///   Subscribes to the broadcast stream.
  ///   <para>
  ///     Registers the subscriber channel, starts the shared WAL loop if it is not already
  ///     running, and waits until the replication slot has been created before returning.
  ///     This guarantees that any DB snapshot the caller performs afterwards will overlap
  ///     with the WAL capture window — no events are lost between snapshot and subscription.
  ///   </para>
  ///   <para>
  ///     The returned <see cref="IAsyncEnumerable{T}" /> streams events from slot creation
  ///     onwards. The loop is stopped when the last subscriber unsubscribes.
  ///   </para>
  /// </summary>
  public async Task<IAsyncEnumerable<T>> SubscribeAsync(CancellationToken cancellationToken)
  {
    var id      = Guid.NewGuid();
    var channel = Channel.CreateUnbounded<T>();

    // Register the channel inside EnsureStartedAndRegister (while holding the lock),
    // after confirming the loop is running. This prevents a cancelling loop's finally
    // block from completing a channel that belongs to the next generation of the loop.
    TaskCompletionSource tcs;
    try
    {
      tcs = await EnsureStartedAndRegister(id,
                                           channel,
                                           cancellationToken)
              .ConfigureAwait(false);
    }
    catch
    {
      channel.Writer.TryComplete();
      throw;
    }

    try
    {
      // Block until the replication slot exists. After this returns, the caller
      // can safely query existing DB state knowing the WAL is capturing events.
      await tcs.Task.WaitAsync(cancellationToken)
               .ConfigureAwait(false);
    }
    catch
    {
      subscribers_.TryRemove(id,
                             out _);
      channel.Writer.TryComplete();
      await StopIfIdle()
        .ConfigureAwait(false);
      throw;
    }

    return ReadChannel(id,
                       channel,
                       cancellationToken);
  }

  private async IAsyncEnumerable<T> ReadChannel(Guid                                          id,
                                                 Channel<T>                                    channel,
                                                 [EnumeratorCancellation] CancellationToken cancellationToken)
  {
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

  // Ensures the broadcast loop is running (starting a new one if needed), registers the
  // subscriber channel while holding the lock, and returns the slot-ready TCS.
  //
  // Registering inside the lock (after verifying the loop is not cancelling) prevents a
  // race where StopIfIdle cancels the loop after the channel is added to subscribers_ but
  // before EnsureStarted runs — in that scenario the old loop's finally would complete the
  // new channel with immediate EOF.
  //
  // When the previous loop is still winding down we wait for it OUTSIDE the lock so we do
  // not block other concurrent SubscribeAsync callers (which would hang if the replication
  // connection takes time to respond to cancellation).
  private async Task<TaskCompletionSource> EnsureStartedAndRegister(Guid              id,
                                                                      Channel<T>        channel,
                                                                      CancellationToken cancellationToken)
  {
    while (true)
    {
      Task? oldLoop = null;

      await lock_.WaitAsync(cancellationToken)
                 .ConfigureAwait(false);
      try
      {
        var loopHealthy = broadcastLoop_ is not null
                       && !broadcastLoop_.IsCompleted
                       && loopCts_?.IsCancellationRequested != true;

        if (loopHealthy)
        {
          // Loop is running and healthy — register and return immediately.
          subscribers_[id] = channel;
          return slotReady_!;
        }

        if (broadcastLoop_ is not null && !broadcastLoop_.IsCompleted)
        {
          // Cancellation was requested but the loop isn't done yet. Capture it to await
          // OUTSIDE the lock so we don't block other subscribers from making progress.
          oldLoop = broadcastLoop_;
        }
        else
        {
          // Loop has finished (or was never started) — safe to start the new one.
          slotReady_       = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
          loopCts_         = new CancellationTokenSource();
          broadcastLoop_   = RunLoop(loopCts_.Token);
          subscribers_[id] = channel;
          return slotReady_!;
        }
      }
      finally
      {
        ReleaseLockSafe();
      }

      // Old loop is still shutting down — wait for it without holding the lock, then retry.
      await oldLoop!.WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
    }
  }

  private async Task StopIfIdle()
  {
    // lock_.WaitAsync() with no cancellation token throws ObjectDisposedException if the
    // broadcaster has been disposed. Catch it so ReadChannel.finally exits cleanly.
    bool acquired;
    try
    {
      await lock_.WaitAsync()
                 .ConfigureAwait(false);
      acquired = true;
    }
    catch (ObjectDisposedException)
    {
      return;
    }

    if (!acquired)
      return;

    try
    {
      if (subscribers_.IsEmpty)
      {
        try
        {
          loopCts_?.Cancel();
        }
        catch (ObjectDisposedException) { }
      }
    }
    finally
    {
      ReleaseLockSafe();
    }
  }

  // SemaphoreSlim.Release() throws ObjectDisposedException when Dispose() has already
  // run and another thread is simultaneously in the Release path. Swallow it here: the
  // semaphore is being torn down and the lock invariant no longer needs to hold.
  private void ReleaseLockSafe()
  {
    try
    {
      lock_.Release();
    }
    catch (ObjectDisposedException) { }
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

      // Slot exists — subscribers can now safely snapshot the DB.
      slotReady_?.TrySetResult();

      await foreach (var message in replConn.StartReplication(slot,
                                                              options,
                                                              cancellationToken)
                                            .ConfigureAwait(false))
      {
        var item = await tryParse_(message,
                                   cancellationToken)
                     .ConfigureAwait(false);
        if (item is not null)
        {
          foreach (var (_, ch) in subscribers_)
            ch.Writer.TryWrite(item);
        }

        // Acknowledge after writing so a subscriber that disconnects between these
        // two points does not cause the event to be both acknowledged and undelivered.
        replConn.SetReplicationStatus(message.WalEnd);
      }
    }
    catch (OperationCanceledException)
    {
      // Normal shutdown (loopCts_ cancelled) or server-side connection reset
      // (e.g. wal_sender_timeout) — Npgsql raises OperationCanceledException in
      // both cases. Complete channels cleanly so consumers can reconnect.
    }
    catch (Exception ex)
    {
      // Slot creation or streaming failed — unblock any waiting SubscribeAsync callers.
      slotReady_?.TrySetException(ex);
      loopException = ex;
    }
    finally
    {
      // If the loop exited before CreatePgOutputReplicationSlot succeeded (e.g. an
      // OperationCanceledException that was caught silently above), slotReady_ may still
      // be unresolved.  Unblock any SubscribeAsync caller waiting on tcs.Task so it can
      // clean up and retry rather than hanging until its cancellation token fires.
      slotReady_?.TrySetCanceled(CancellationToken.None);
      foreach (var (_, ch) in subscribers_)
        ch.Writer.TryComplete(loopException);
    }
  }

  public void Dispose()
  {
    loopCts_?.Cancel();
    loopCts_?.Dispose();
    lock_.Dispose();
    // Concurrent StopIfIdle / EnsureStartedAndRegister callers handle ObjectDisposedException
    // from lock_.WaitAsync() and loopCts_.Cancel() gracefully — see ReleaseLockSafe and
    // the try/catch in StopIfIdle.
  }
}
