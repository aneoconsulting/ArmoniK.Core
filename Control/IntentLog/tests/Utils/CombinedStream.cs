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

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public class CombinedStream : Stream
{
  private readonly Stream reader_;
  private readonly Stream writer_;

  public CombinedStream(Stream reader,
                        Stream writer)
  {
    if (!reader.CanRead)
    {
      throw new ArgumentException("Must be a readable stream",
                                  nameof(reader));
    }

    if (!writer.CanWrite)
    {
      throw new ArgumentException("Must be a writable stream",
                                  nameof(writer));
    }

    reader_ = reader;
    writer_ = writer;
  }


  public override bool CanRead
    => reader_.CanRead;

  public override bool CanWrite
    => writer_.CanWrite;

  public override bool CanSeek
    => false;

  public override bool CanTimeout
    => reader_.CanTimeout || writer_.CanTimeout;

  public override long Length
    => throw new NotSupportedException();

  public override long Position
  {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }

  public override int ReadTimeout
  {
    get => reader_.ReadTimeout;
    set => reader_.ReadTimeout = value;
  }

  public override int WriteTimeout
  {
    get => writer_.WriteTimeout;
    set => writer_.WriteTimeout = value;
  }

  public override void Close()
  {
    try
    {
      reader_.Close();
      writer_.Close();
    }
    finally
    {
      base.Dispose(true);
    }
  }

  public override void CopyTo(Stream destination,
                              int    bufferSize)
    => reader_.CopyTo(destination,
                      bufferSize);

  public override Task CopyToAsync(Stream            destination,
                                   int               bufferSize,
                                   CancellationToken cancellationToken)
    => reader_.CopyToAsync(destination,
                           bufferSize,
                           cancellationToken);

  protected override void Dispose(bool disposing)
  {
    try
    {
      if (!disposing)
      {
        return;
      }

      reader_.Dispose();
      writer_.Dispose();
    }
    finally
    {
      base.Dispose(disposing);
    }
  }

  public override ValueTask DisposeAsync()
  {
    var disposeReader = reader_.DisposeAsync();
    var disposeWriter = writer_.DisposeAsync();

    return (disposeReader.IsCompletedSuccessfully, disposeWriter.IsCompletedSuccessfully) switch
           {
             (false, false) => new ValueTask(Task.WhenAll(disposeReader.AsTask(),
                                                          disposeWriter.AsTask())),
             (false, true) => disposeReader,
             (true, false) => disposeWriter,
             (true, true)  => ValueTask.CompletedTask,
           };
  }

  public override void Flush()
  {
    reader_.Flush();
    writer_.Flush();
  }

  public override Task FlushAsync(CancellationToken cancellationToken)
    => Task.WhenAll(reader_.FlushAsync(cancellationToken),
                    writer_.FlushAsync(cancellationToken));

  public override int Read(byte[] bytes,
                           int    offset,
                           int    count)
    => reader_.Read(bytes,
                    offset,
                    count);

  public override int Read(Span<byte> buffer)
    => reader_.Read(buffer);

  public override int ReadByte()
    => reader_.ReadByte();

  public override IAsyncResult BeginRead(byte[]         buffer,
                                         int            offset,
                                         int            count,
                                         AsyncCallback? callback,
                                         object?        state)
    => reader_.BeginRead(buffer,
                         offset,
                         count,
                         callback,
                         state);

  public override int EndRead(IAsyncResult asyncResult)
    => reader_.EndRead(asyncResult);

  public override Task<int> ReadAsync(byte[]            buffer,
                                      int               offset,
                                      int               count,
                                      CancellationToken cancellationToken)
    => reader_.ReadAsync(buffer,
                         offset,
                         count,
                         cancellationToken);

  public override ValueTask<int> ReadAsync(Memory<byte>      buffer,
                                           CancellationToken cancellationToken = new())
    => reader_.ReadAsync(buffer,
                         cancellationToken);

  public override long Seek(long       offset,
                            SeekOrigin origin)
    => throw new NotSupportedException();

  public override void SetLength(long length)
    => throw new NotSupportedException();

  public override void Write(byte[] bytes,
                             int    offset,
                             int    count)
    => writer_.Write(bytes,
                     offset,
                     count);

  public override void Write(ReadOnlySpan<byte> buffer)
    => writer_.Write(buffer);

  public override void WriteByte(byte b)
    => writer_.WriteByte(b);

  public override IAsyncResult BeginWrite(byte[]         buffer,
                                          int            offset,
                                          int            count,
                                          AsyncCallback? callback,
                                          object?        state)
    => writer_.BeginWrite(buffer,
                          offset,
                          count,
                          callback,
                          state);

  public override void EndWrite(IAsyncResult asyncResult)
    => writer_.EndWrite(asyncResult);

  public override Task WriteAsync(byte[]            buffer,
                                  int               offset,
                                  int               count,
                                  CancellationToken cancellationToken)
    => writer_.WriteAsync(buffer,
                          offset,
                          count,
                          cancellationToken);

  public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
                                       CancellationToken    cancellationToken = new())
    => writer_.WriteAsync(buffer,
                          cancellationToken);
}
