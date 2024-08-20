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
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(RendezVousChannel<>))]
public class RendezVousChannelTest
{
  [Test]
  [Timeout(1000)]
  [Repeat(10000)]
  public async Task WriteShouldWork([Values(0,
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
    var queue = new RendezVousChannel<int>();

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
    Assert.That(read,
                Is.EqualTo(Enumerable.Range(0,
                                            n)
                                     .ToList()));
  }

  private static async IAsyncEnumerable<int> ReadAsync(RendezVousChannel<int> queue,
                                                       int                    closeAfter)
  {
    var nbRead = 0;
    while (nbRead < closeAfter)
    {
      int x;
      try
      {
        x = await queue.ReadAsync(Timeout.InfiniteTimeSpan,
                                  CancellationToken.None)
                       .ConfigureAwait(false);
      }
      catch (ChannelClosedException)
      {
        break;
      }

      yield return x;
      nbRead++;
    }

    queue.CloseReader();
  }

  private static async Task<int> WriteAsync(RendezVousChannel<int> queue,
                                            int                    closeAfter)
  {
    var nbWritten = 0;

    while (nbWritten < closeAfter)
    {
      try
      {
        await queue.WriteAsync(nbWritten,
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
