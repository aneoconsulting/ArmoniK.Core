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
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Protocol.Messages;
using ArmoniK.Utils;

using JetBrains.Annotations;

using Action = ArmoniK.Core.Control.IntentLog.Protocol.Messages.Action;

namespace ArmoniK.Core.Control.IntentLog.Protocol.Server;

[PublicAPI]
public class Connection<T> : IDisposable, IAsyncDisposable
  where T : class
{
  private CancellationTokenSource cts_;
  private Task                    eventLoop_;

  [PublicAPI]
  public Connection(IServerHandler<T> handler,
                    Stream            stream,
                    System.Action     onClose,
                    CancellationToken cancellationToken)
  {
    cts_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    var responseChannel = Channel.CreateBounded<Response>(1);

    eventLoop_ = Task.Run(EventLoop);


    return;

    async Task EventLoop()
    {
      using var       cts = cts_;
      await using var str = stream;

      // ReSharper disable once VariableHidesOuterVariable
      var cancellationToken = cts_.Token;

      var nextRequest  = NextRequest();
      var nextResponse = NextResponse();

      var mapping = new Dictionary<int, Queue<Task<bool>>>();

      while (!cancellationToken.IsCancellationRequested)
      {
        try
        {
          var readyTask = await Task.WhenAny(nextRequest,
                                             nextResponse)
                                    .ConfigureAwait(false);

          if (ReferenceEquals(readyTask,
                              nextRequest))
          {
            var request = await nextRequest.ConfigureAwait(false);

            nextRequest = NextRequest();

            if (!mapping.TryGetValue(request.IntentId,
                                     out var queue))
            {
              queue                     = new Queue<Task<bool>>();
              mapping[request.IntentId] = queue;
            }

            queue.Enqueue(Task.Run(ProcessRequest));

            async Task<bool> ProcessRequest()
            {
              var response = new Response
                             {
                               IntentId = request.IntentId,
                             };

              try
              {
                Func<Connection<T>, int, T?, CancellationToken, Task> f = request.Action switch
                                                                          {
                                                                            Action.Open    => handler.OpenAsync,
                                                                            Action.Amend   => handler.AmendAsync,
                                                                            Action.Close   => handler.CloseAsync,
                                                                            Action.Abort   => handler.AbortAsync,
                                                                            Action.Timeout => handler.TimeoutAsync,
                                                                            Action.Reset   => handler.ResetAsync,
                                                                            _              => throw new InvalidOperationException(),
                                                                          };


                await f(this,
                        request.IntentId,
                        request.Payload,
                        cancellationToken)
                  .ConfigureAwait(false);
              }
              catch (Exception e)
              {
                response.Error = e.Message;
              }

              await responseChannel.Writer.WriteAsync(response,
                                                      cancellationToken)
                                   .ConfigureAwait(false);

              return request.Action.IsFinal();
            }
          }
          else
          {
            var response = await nextResponse.ConfigureAwait(false);
            nextResponse = NextResponse();

            if (mapping.TryGetValue(response.IntentId,
                                    out var queue) && queue.TryDequeue(out var task))
            {
              try
              {
                var final = await task.ConfigureAwait(false);

                if (final && queue.Count == 0)
                {
                  mapping.Remove(response.IntentId);
                }
              }
              catch (Exception e)
              {
                response.Error = e.Message;
              }
            }

            await response.SendAsync(str,
                                     cancellationToken)
                          .ConfigureAwait(false);
          }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
          // Empty on purpose
        }
        catch
        {
          await cts_.CancelAsync()
                    .ConfigureAwait(false);
          onClose();
        }
      }

      foreach (var (id, queue) in mapping)
      {
        var final = false;
        foreach (var task in queue)
        {
          try
          {
            final = await task.ConfigureAwait(false);
          }
          catch
          {
            // TODO: log
          }
        }

        if (final)
        {
          continue;
        }

        try
        {
          await handler.ResetAsync(this,
                                   id,
                                   null,
                                   cancellationToken)
                       .ConfigureAwait(false);
        }
        catch
        {
          // TODO: log
        }
      }
    }

    Task<Request<T>> NextRequest()
      => Request<T>.ReceiveAsync(stream,
                                 cts_.Token);

    Task<Response> NextResponse()
      => responseChannel.Reader.ReadAsync(cts_.Token)
                        .AsTask();
  }

  public async ValueTask DisposeAsync()
  {
    try
    {
      await cts_.CancelAsync()
                .ConfigureAwait(false);
    }
    catch (ObjectDisposedException)
    {
      // Empty on purpose
    }

    try
    {
      await eventLoop_.ConfigureAwait(false);
    }
    catch
    {
      // TODO: log
    }

    cts_.Dispose();
  }


  public void Dispose()
    => DisposeAsync()
      .WaitSync();
}
