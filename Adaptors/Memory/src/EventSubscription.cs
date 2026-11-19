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
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   Turns the change events of an in-memory table into a stream of watch events
/// </summary>
internal static class EventSubscription
{
  /// <summary>
  ///   Subscribe to the change events of a table, and convert them into watch events
  /// </summary>
  /// <remarks>
  ///   <para>
  ///     The subscription is made before this method returns, so a change made after the call is not lost, even if the
  ///     returned enumerable is enumerated later.
  ///   </para>
  ///   <para>
  ///     The handler runs on the writing thread, under the table lock: it only converts the change and writes it into an
  ///     unbounded channel, without blocking.
  ///   </para>
  ///   <para>
  ///     The handler is removed when <paramref name="cancellationToken" /> is cancelled, or when the enumeration of the
  ///     returned enumerable ends.
  ///   </para>
  /// </remarks>
  /// <typeparam name="TValue">Type of the values stored in the table</typeparam>
  /// <typeparam name="TEvent">Type of the watch events</typeparam>
  /// <param name="add">Adds a handler to the change event of the table</param>
  /// <param name="remove">Removes a handler from the change event of the table</param>
  /// <param name="convert">
  ///   Converts a change (previous value, null on creation, and new value) into a watch event, or null when the change does
  ///   not produce any event. A conversion that throws (for instance a filter that cannot be evaluated on the value) is
  ///   treated as producing no event.
  /// </param>
  /// <param name="logger">Logger used to report conversions that throw</param>
  /// <param name="cancellationToken">Token used to end the subscription</param>
  /// <returns>
  ///   The watch events produced by the changes made after the call
  /// </returns>
  public static IAsyncEnumerable<TEvent> Subscribe<TValue, TEvent>(Action<Action<TValue?, TValue>> add,
                                                                   Action<Action<TValue?, TValue>> remove,
                                                                   Func<TValue?, TValue, TEvent?>  convert,
                                                                   ILogger                         logger,
                                                                   CancellationToken               cancellationToken)
    where TValue : class
    where TEvent : class
  {
    var channel = Channel.CreateUnbounded<TEvent>(new UnboundedChannelOptions
                                                  {
                                                    SingleReader = true,
                                                  });

    Action<TValue?, TValue> handler = (previous,
                                       current) =>
                                      {
                                        TEvent? watchEvent;
                                        try
                                        {
                                          watchEvent = convert(previous,
                                                               current);
                                        }
                                        catch (Exception e)
                                        {
                                          logger.LogDebug(e,
                                                          "Change of {Type} ignored by a watch: it cannot be converted into {Event}",
                                                          typeof(TValue).Name,
                                                          typeof(TEvent).Name);
                                          return;
                                        }

                                        if (watchEvent is not null)
                                        {
                                          channel.Writer.TryWrite(watchEvent);
                                        }
                                      };

    var unsubscribed = 0;

    void Unsubscribe()
    {
      if (Interlocked.Exchange(ref unsubscribed,
                               1) == 0)
      {
        remove(handler);
      }
    }

    add(handler);
    var registration = cancellationToken.Register(Unsubscribe);

    return Read(channel.Reader,
                Unsubscribe,
                registration,
                cancellationToken);
  }

  private static async IAsyncEnumerable<TEvent> Read<TEvent>(ChannelReader<TEvent>                      reader,
                                                             Action                                     unsubscribe,
                                                             CancellationTokenRegistration              registration,
                                                             [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    try
    {
      await foreach (var watchEvent in reader.ReadAllAsync(cancellationToken)
                                             .ConfigureAwait(false))
      {
        yield return watchEvent;
      }
    }
    finally
    {
      await registration.DisposeAsync()
                        .ConfigureAwait(false);
      unsubscribe();
    }
  }
}
