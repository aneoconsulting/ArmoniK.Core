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

using Microsoft.Extensions.Logging;

using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace ArmoniK.Core.Adapters.PostgreSQL.Common;

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
///     The connection is closed when the last subscriber disconnects, or once it has stayed without
///     subscriber for <see cref="Options.PostgreSQL.ReplicationIdleTimeout" /> when it is positive.
///     A later subscriber opens a new connection, in a new <see cref="Generation" />.
///   </para>
/// </summary>
/// <typeparam name="T">The decoded event payload type (e.g. TaskData, Result).</typeparam>
internal sealed class WalBroadcaster<T> : IDisposable
  where T : class
{
  private readonly TimeSpan idleTimeout_;

  // Every section under this lock is synchronous: a plain lock is enough, and it needs no disposal.
  private readonly Lock                                                          lock_ = new();
  private readonly ILogger                                                       logger_;
  private readonly string                                                        name_;
  private readonly NpgsqlConnectionProvider                                      provider_;
  private readonly Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse_;
  private readonly IUuidGenerator                                                uuidGenerator_;

  // Generation that new subscribers join while it is open. Read and written under lock_ only.
  private Generation? current_;
  private bool        disposed_;

  internal WalBroadcaster(NpgsqlConnectionProvider                                      provider,
                          Func<PgOutputReplicationMessage, CancellationToken, Task<T?>> tryParse,
                          IUuidGenerator                                                uuidGenerator,
                          string                                                        name,
                          ILogger                                                       logger)
  {
    name_          = name;
    logger_        = logger;
    provider_      = provider;
    tryParse_      = tryParse;
    uuidGenerator_ = uuidGenerator;
    idleTimeout_   = provider.ReplicationIdleTimeout;
  }

  public void Dispose()
  {
    lock (lock_)
    {
      disposed_ = true;
      // The loop completes the channels of its remaining subscribers once it is cancelled.
      current_?.Stop();
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

  // Adds the subscriber to the current generation, or to a new one if it is stopped.
  private Generation Register(Guid       id,
                              Channel<T> channel)
  {
    lock (lock_)
    {
      ObjectDisposedException.ThrowIf(disposed_,
                                      this);

      if (current_ is not
          {
            IsStopped: false,
          })
      {
        current_ = new Generation(RunLoop);
      }

      // Joining invalidates the pending idle timeout of the generation, if any.
      current_.IdleVersion++;
      current_.Subscribers[id] = channel;
      return current_;
    }
  }

  // Removes the subscriber from its own generation, and stops that generation once it has no
  // subscriber left, right away or after the idle timeout. A newer generation is never affected.
  private void Unsubscribe(Generation generation,
                           Guid       id,
                           Channel<T> channel)
  {
    channel.Writer.TryComplete();

    lock (lock_)
    {
      generation.Subscribers.TryRemove(id,
                                       out _);
      if (!generation.Subscribers.IsEmpty)
      {
        return;
      }

      if (idleTimeout_ > TimeSpan.Zero && !generation.IsStopped)
      {
        // Keep the replication connection open for a while: a new subscriber can reuse it
        // instead of creating a new replication slot.
        _ = StopAfterIdleTimeout(generation,
                                 ++generation.IdleVersion);
      }
      else
      {
        generation.Stop();
      }
    }
  }

  // Stops the generation once the idle timeout has elapsed, unless a subscriber joined it in the
  // meantime, which changed its IdleVersion. Ends early when the generation is stopped otherwise.
  private async Task StopAfterIdleTimeout(Generation generation,
                                          long       idleVersion)
  {
    try
    {
      await Task.Delay(idleTimeout_,
                       generation.CancellationToken)
                .ConfigureAwait(false);
    }
    catch (OperationCanceledException)
    {
      return;
    }

    lock (lock_)
    {
      if (generation.IdleVersion == idleVersion && generation.Subscribers.IsEmpty)
      {
        generation.Stop();
      }
    }
  }

  private async Task RunLoop(Generation generation)
  {
    var                           cancellationToken = generation.CancellationToken;
    Exception?                    loopException     = null;
    LogicalReplicationConnection? replConn          = null;
    try
    {
      var slotName = $"armonik_{uuidGenerator_.GenerateUuid():N}";
      var options = new PgOutputReplicationOptions("armonik_pub",
                                                   PgOutputProtocolVersion.V1,
                                                   true);

      replConn = provider_.CreateReplicationConnection();
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
    catch (OperationCanceledException ex)
    {
      // Normal shutdown (generation cancelled) or server-side connection reset
      // (e.g. wal_sender_timeout) — Npgsql raises OperationCanceledException in
      // both cases. Complete channels cleanly so consumers can reconnect.
      if (!cancellationToken.IsCancellationRequested)
      {
        logger_.LogWarning(ex,
                           "WAL replication for {broadcaster} was interrupted by the server",
                           name_);
      }
    }
    catch (Exception ex)
    {
      logger_.LogError(ex,
                       "WAL replication for {broadcaster} failed",
                       name_);
      // Slot creation or streaming failed — unblock any waiting SubscribeAsync callers.
      generation.SlotReady.TrySetException(ex);
      loopException = ex;
    }
    finally
    {
      // If the loop exited before the slot was created, unblock any SubscribeAsync caller
      // still waiting on SlotReady so it can clean up and retry.
      generation.SlotReady.TrySetCanceled(CancellationToken.None);

      // Stop the generation and complete its channels under the lock: a subscriber either joined
      // before, and its channel is completed here, or it sees the generation stopped and starts a
      // new one. A stopped generation is never joined, so a new one can start right away.
      lock (lock_)
      {
        generation.Stop();
        foreach (var (_, ch) in generation.Subscribers)
        {
          ch.Writer.TryComplete(loopException);
        }
      }

      // Disposed only once the generation is stopped: disposing a broken connection can take time,
      // and no subscriber must join the generation meanwhile.
      if (replConn is not null)
      {
        await replConn.DisposeAsync()
                      .AsTask()
                      .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
      }
    }
  }

  /// <summary>
  ///   One replication connection and the subscribers it serves, from the first subscriber to the
  ///   end of its loop.
  /// </summary>
  private sealed class Generation
  {
    private readonly Func<Task> cancelAsync_;

    // Completes once the callbacks of the requested cancellation, if any, have run. Set under
    // lock_ by Stop.
    private Task cancellation_ = Task.CompletedTask;

    // Starts the loop of the generation. The generation owns its token source: the token source
    // is disposed once the loop has ended and the callbacks of its cancellation have run.
    public Generation(Func<Generation, Task> run)
    {
      var cts = new CancellationTokenSource();
      CancellationToken = cts.Token;
      cancelAsync_      = cts.CancelAsync;

      // Task.Run so that no code of the loop runs under the lock. Not cancellable: the loop must
      // always run, since it completes the subscribers and the token source must be disposed.
      _ = Task.Run(async () =>
                   {
                     try
                     {
                       await run(this)
                         .ConfigureAwait(false);
                     }
                     finally
                     {
                       // The loop has stopped the generation under lock_: nobody cancels it anymore.
                       // A failing cancellation callback must not prevent the disposal.
                       await cancellation_.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                       cts.Dispose();
                     }
                   },
                   CancellationToken.None);
    }

    public CancellationToken CancellationToken { get; }

    public TaskCompletionSource SlotReady { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    // Written under lock_, read without it by the loop.
    public ConcurrentDictionary<Guid, Channel<T>> Subscribers { get; } = new();

    // Incremented, under lock_, when the generation starts waiting for its idle timeout and when a
    // subscriber joins it, so that a pending idle timeout knows whether it still applies.
    public long IdleVersion { get; set; }

    // A stopped generation is never joined. Stopped by Stop only, under lock_, so reading it under
    // lock_ is consistent. Still readable once the token source is disposed.
    public bool IsStopped
      => CancellationToken.IsCancellationRequested;

    // Stops the generation by cancelling its token, once. Called when its last subscriber leaves,
    // on Dispose, and by its loop when it ends. Must be called while holding lock_: CancelAsync
    // runs the token callbacks, such as those of Npgsql, off the lock.
    public void Stop()
    {
      if (!IsStopped)
      {
        cancellation_ = cancelAsync_();
      }
    }
  }
}
