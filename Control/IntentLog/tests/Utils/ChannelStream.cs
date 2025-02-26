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

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public class ChannelStream
{
  public static (Stream, Stream) CreatePair(int capacity = 0)
  {
    var writer1 = new ChannelStreamProducer(capacity);
    var writer2 = new ChannelStreamProducer(capacity);
    var reader1 = new ChannelStreamConsumer(writer1.Reader);
    var reader2 = new ChannelStreamConsumer(writer2.Reader);

    var stream1 = new CombinedStream(reader1,
                                     writer2);
    var stream2 = new CombinedStream(reader2,
                                     writer1);

    return (stream1, stream2);
  }
}
