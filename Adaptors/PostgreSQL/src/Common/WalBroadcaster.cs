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

using ArmoniK.Core.Utils.Uuid;

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgreSQL.Common;

/// <summary>
///   Holds a single shared WAL replication connection for one event type and fans out decoded
///   events to all active subscribers via in-memory channels: a shared, reference counted source.
///   <para>
///     The replication connection and slot are opened when the first subscriber registers.
///     The slot creation is signalled via an internal <see cref="TaskCompletionSource" /> so
///     that callers can wait until the slot exists before performing an initial DB snapshot —
///     this eliminates the race window where events between snapshot and slot creation would
///     otherwise be lost.
///   </para>
///   <para>
///     The connection is closed when the last subscriber disconnects. A later subscriber
///     opens a new connection, in a new <see cref="Generation" />.
///   </para>
/// </summary>
/// <typeparam name="T">The decoded event payload type (e.g. TaskData, Result).</typeparam>
internal sealed class WalBroadcaster<T> : IDisposable
  where T : class
{
  // Every section under this lock is synchronous: a plain lock is enough, and it needs no disposal.
  private readonly Lock lock_ = new();

  private readonly NpgsqlConnectionProvider                                      provider_;
  private readonly Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse_;
  private readonly IUuidGenerator                                                uuidGenerator_;

  // Generation that new subscribers join while it is open. Read and written under lock_ only.
  private Generation? current_;
  private bool        disposed_;

  internal WalBroadcaster(NpgsqlConnectionProvider                                      provider,
                          Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse,
                          IUuidGenerator                                                uuidGenerator)
  {
    provider_      = provider;
    tryParse_      = tryParse;
    uuidGenerator_ = uuidGenerator;
  }

  public void Dispose()
  {
    lock (lock_)
    {
      disposed_ = true;
      // The loop completes the channels of its remaining subscribers once it is cancelled.
      Stop(current_);
    }
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
    var id      = uuidGenerator_.GenerateUuid();
    var channel = Channel.CreateUnbounded<T>();
    var generation = Register(id,
                              channel);

    try
    {
      // Block until the replication slot exists. After this returns, the caller
      // can safely query existing DB state knowing the WAL is capturing events.
      await generation.SlotReady.Task.WaitAsync(cancellationToken)
                      .ConfigureAwait(false);
    }
    catch
    {
      Unsubscribe(generation,
                  id,
                  channel);
      throw;
    }

    return ReadChannel(generation,
                       id,
                       channel,
                       cancellationToken);
  }

  private async IAsyncEnumerable<T> ReadChannel(Generation                                 generation,
                                                Guid                                       id,
                                                Channel<T>                                 channel,
                                                [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    try
    {
      await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken)
                                        .ConfigureAwait(false))
      {
        yield return item;
      }
    }
    finally
    {
      Unsubscribe(generation,
                  id,
                  channel);
    }
  }

  // Adds the subscriber to the open generation, or to a new one started for it.
  // A generation that is closed (stopping or dead) is never joined: its loop completes the
  // channels of its own subscribers only, so a new generation can start right away.
  private Generation Register(Guid       id,
                              Channel<T> channel)
  {
    lock (lock_)
    {
      ObjectDisposedException.ThrowIf(disposed_,
                                      this);

      if (current_ is not
          {
            Open: true,
          })
      {
        // The loop owns the token source and disposes it; the generation only cancels it.
        var cts = new CancellationTokenSource();
        var generation = new Generation(cts.CancelAsync,
                                        cts.Token);
        current_ = generation;
        // Task.Run so that no code of the loop runs under the lock.
        _ = Task.Run(() => RunLoop(generation,
                                   cts));
      }

      current_.Subscribers[id] = channel;
      return current_;
    }
  }

  // Removes the subscriber from its own generation, and stops that generation once it has no
  // subscriber left. A newer generation is never affected.
  private void Unsubscribe(Generation generation,
                           Guid       id,
                           Channel<T> channel)
  {
    channel.Writer.TryComplete();

    lock (lock_)
    {
      generation.Subscribers.TryRemove(id,
                                       out _);
      if (generation.Subscribers.IsEmpty)
      {
        Stop(generation);
      }
    }
  }

  // Closes the generation to new subscribers. Returns whether it was open. Must be called while
  // holding lock_.
  private static bool Close(Generation? generation)
  {
    if (generation is not
        {
          Open: true,
        })
    {
      return false;
    }

    generation.Open = false;
    return true;
  }

  // Closes the generation and cancels its loop, unless it was already closed. CancelAsync only
  // flags the token synchronously and runs the callbacks registered on it, such as those of
  // Npgsql, asynchronously: no work is done under the lock. Must be called while holding lock_.
  private static void Stop(Generation? generation)
  {
    if (Close(generation))
    {
      generation!.Cancellation = generation.CancelAsync();
    }
  }

  private async Task RunLoop(Generation              generation,
                             CancellationTokenSource cts)
  {
    var        cancellationToken = generation.CancellationToken;
    Exception? loopException     = null;
    try
    {
      var slotName = $"armonik_{uuidGenerator_.GenerateUuid():N}";
      var options = new PgOutputReplicationOptions("armonik_pub",
                                                   PgOutputProtocolVersion.V1,
                                                   true);

      await using var replConn = provider_.CreateReplicationConnection();
      await replConn.Open(cancellationToken)
                    .ConfigureAwait(false);

      var slot = await replConn.CreatePgOutputReplicationSlot(slotName,
                                                              true,
                                                              cancellationToken: cancellationToken)
                               .ConfigureAwait(false);

      // Slot exists — subscribers can now safely snapshot the DB.
      generation.SlotReady.TrySetResult();

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
          foreach (var (_, ch) in generation.Subscribers)
          {
            ch.Writer.TryWrite(item);
          }
        }

        // Acknowledge after writing so a subscriber that disconnects between these
        // two points does not cause the event to be both acknowledged and undelivered.
        replConn.SetReplicationStatus(message.WalEnd);
      }
    }
    catch (OperationCanceledException)
    {
      // Normal shutdown (generation cancelled) or server-side connection reset
      // (e.g. wal_sender_timeout) — Npgsql raises OperationCanceledException in
      // both cases. Complete channels cleanly so consumers can reconnect.
    }
    catch (Exception ex)
    {
      // Slot creation or streaming failed — unblock any waiting SubscribeAsync callers.
      generation.SlotReady.TrySetException(ex);
      loopException = ex;
    }
    finally
    {
      // If the loop exited before the slot was created, unblock any SubscribeAsync caller
      // still waiting on SlotReady so it can clean up and retry.
      generation.SlotReady.TrySetCanceled(CancellationToken.None);

      // Close the generation and complete its channels under the lock: a subscriber either
      // joined before, and its channel is completed here, or it sees the generation closed and
      // starts a new one.
      lock (lock_)
      {
        Close(generation);
        foreach (var (_, ch) in generation.Subscribers)
        {
          ch.Writer.TryComplete(loopException);
        }
      }

      // The generation is closed, so nobody cancels it anymore. Wait for the callbacks of a
      // cancellation that was requested, then dispose the token source.
      try
      {
        await generation.Cancellation.ConfigureAwait(false);
      }
      catch (Exception)
      {
        // A failing cancellation callback must not prevent the disposal.
      }

      cts.Dispose();
    }
  }

  /// <summary>
  ///   One replication connection and the subscribers it serves, from the first subscriber to the
  ///   end of its loop.
  /// </summary>
  private sealed class Generation(Func<Task>        cancelAsync,
                                  CancellationToken cancellationToken)
  {
    // Requests the cancellation of the loop, see Stop.
    public readonly Func<Task> CancelAsync = cancelAsync;

    // Token of the loop. The loop owns its token source, and disposes it once the generation is
    // closed: from then on, nobody cancels it anymore.
    public readonly CancellationToken CancellationToken = cancellationToken;

    public readonly TaskCompletionSource SlotReady = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Written under lock_, read without it by the loop.
    public readonly ConcurrentDictionary<Guid, Channel<T>> Subscribers = new();

    // Completes once the callbacks of the requested cancellation, if any, have run. Set under
    // lock_ when the generation is stopped.
    public Task Cancellation = Task.CompletedTask;

    // Whether new subscribers may join. Set to false, under lock_, when the generation is
    // stopped or its loop ends.
    public bool Open = true;
  }
}
