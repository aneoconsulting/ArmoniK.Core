// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(TaskQueueBase))]
public class TaskQueueTest
{
  public sealed class TaskQueue : TaskQueueBase;

  [Test]
  [Timeout(1000)]
  [Repeat(100)]
  public async Task Pouet([Values(0,
                                  1,
                                  2,
                                  3)]
                          int nbRead,
                          [Values(0,
                                  1,
                                  2,
                                  3)]
                          int nbWrite)
  {
    var queue = new TaskQueue();
    queue.MinDelay = 0;
    queue.MaxDelay = 0;

    var reader = ReadAsync(queue,
                           nbRead)
                 .ToListAsync(CancellationToken.None)
                 .AsTask();
    var writer = WriteAsync(queue,
                            nbWrite);

    var n = int.Min(nbRead,
                    nbWrite);

    Assert.That(() => Task.WhenAll(reader,
                                   writer),
                Throws.Nothing);

    var read    = await reader.ConfigureAwait(false);
    var written = await writer.ConfigureAwait(false);

    Assert.That(read,
                Has.Count.EqualTo(n));
    Assert.That(written,
                Is.EqualTo(n));
  }

  private static TaskHandler CreateTaskHandler(string podId)
    => new(new SimpleSessionTable(),
           new SimpleTaskTable(),
           new SimpleResultTable(),
           new SimpleSubmitter(),
           new DataPrefetcher(new SimpleObjectStorage(),
                              null,
                              NullLogger<DataPrefetcher>.Instance),
           new SimpleWorkerStreamHandler(),
           new SimpleQueueMessageHandler(),
           new HelperTaskProcessingChecker(),
           podId,
           "",
           new ActivitySource(""),
           new SimpleAgentHandler(),
           NullLogger.Instance,
           new Injection.Options.Pollster(),
           () =>
           {
           },
           new ApplicationLifetime(NullLogger<ApplicationLifetime>.Instance),
           null!);

  private static async IAsyncEnumerable<TaskHandler> ReadAsync(TaskQueueBase queue,
                                                               int           closeAfter)
  {
    var nbRead = 0;
    while (nbRead < closeAfter)
    {
      TaskHandler taskHandler;
      try
      {
        taskHandler = await queue.ReadAsync(Timeout.InfiniteTimeSpan,
                                            CancellationToken.None)
                                 .ConfigureAwait(false);
      }
      catch (ChannelClosedException)
      {
        break;
      }

      yield return taskHandler;
      nbRead++;
    }

    queue.CloseReader();
  }

  private static async Task<int> WriteAsync(TaskQueueBase queue,
                                            int           closeAfter)
  {
    var nbWritten = 0;

    while (nbWritten < closeAfter)
    {
      try
      {
        await queue.WriteAsync(CreateTaskHandler(nbWritten.ToString()),
                               Timeout.InfiniteTimeSpan,
                               CancellationToken.None)
                   .ConfigureAwait(false);
        nbWritten++;
      }
      catch (ChannelClosedException)
      {
        break;
      }
    }

    queue.CloseWriter();

    return nbWritten;
  }
}
