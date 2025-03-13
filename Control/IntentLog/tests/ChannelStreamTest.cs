// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-$CURRENT_YEAR.All rights reserved.
// 
// This program is free software:you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.If not, see <http://www.gnu.org/licenses/>.

using System.IO;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Core.Control.IntentLog.Tests.Utils;

using NUnit.Framework;

namespace ArmoniK.Core.Control.IntentLog.Tests;

[TestFixture(TestOf = typeof(ChannelStream))]
public class ChannelStreamTest
{
  [Test]
  [Timeout(1000)]
  public async Task Stream()
  {
    var (reader, writer) = ChannelStream.CreatePair();
    await writer.WriteAsync("Hello"u8.ToArray());
    await writer.WriteAsync(" world!"u8.ToArray());
    var buffer = new byte[16];

    var read = await reader.ReadAsync(buffer);

    Assert.That(read,
                Is.EqualTo(12));
    Assert.That(Encoding.UTF8.GetString(buffer[..12]),
                Is.EqualTo("Hello world!"));

    var readTask = reader.ReadAsync(buffer);
    await Task.Delay(10);

    Assert.That(readTask.IsCompleted,
                Is.False);

    await writer.WriteAsync("This is a long sentence."u8.ToArray());

    read = await readTask;
    Assert.That(read,
                Is.EqualTo(16));
    Assert.That(Encoding.UTF8.GetString(buffer),
                Is.EqualTo("This is a long s"));

    writer.Close();

    read = await reader.ReadAsync(buffer);
    Assert.That(Encoding.UTF8.GetString(buffer[..8]),
                Is.EqualTo("entence."));
    Assert.That(read,
                Is.EqualTo(8));

    Assert.That(reader.Read(buffer),
                Is.Zero);
    Assert.That(() => reader.ReadExactlyAsync(buffer),
                Throws.InstanceOf<EndOfStreamException>());
  }
}
