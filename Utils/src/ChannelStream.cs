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
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Utils;

namespace ArmoniK.Core.Utils;

/// <summary>
///   Converts channels into <see cref="Stream" />
/// </summary>
public class ChannelStream : Stream
{
  private readonly ChannelReader<byte[]> reader_;
  private readonly ChannelWriter<byte[]> writer_;
  private          ReadOnlyMemory<byte>  currentRead_;

  /// <summary>
  ///   Creates a new <see cref="Stream" /> from a pair <see cref="ChannelReader{T}" /> / <see cref="ChannelWriter{T}" />.
  /// </summary>
  /// <param name="reader">Channel where bytes are read from.</param>
  /// <param name="writer">Channel where bytes are written to.</param>
  public ChannelStream(ChannelReader<byte[]> reader,
                       ChannelWriter<byte[]> writer)
  {
    reader_      = reader;
    writer_      = writer;
    currentRead_ = ReadOnlyMemory<byte>.Empty;
  }

  /// <inheritdoc />
  public override bool CanRead
    => true;

  /// <inheritdoc />
  public override bool CanSeek
    => false;

  /// <inheritdoc />
  public override bool CanWrite
    => true;

  /// <inheritdoc />
  public override long Length
    => throw new NotSupportedException();

  /// <inheritdoc />
  public override long Position
  {
    get => throw new NotSupportedException();
    set => throw new NotSupportedException();
  }

  /// <inheritdoc />
  public override void Flush()
  {
  }

  /// <inheritdoc />
  public override int Read(byte[] buffer,
                           int    offset,
                           int    count)
    => Read(buffer.AsSpan(offset,
                          count));

  /// <inheritdoc />
  public override int Read(Span<byte> buffer)
  {
    WaitForBuffer()
      .WaitSync();
    return ReadBuffer(buffer);
  }

  /// <inheritdoc />
  public override Task<int> ReadAsync(byte[]            buffer,
                                      int               offset,
                                      int               count,
                                      CancellationToken cancellationToken)
    => ReadAsync(buffer.AsMemory(offset,
                                 count),
                 cancellationToken)
      .AsTask();

  /// <inheritdoc />
  public override ValueTask<int> ReadAsync(Memory<byte>      buffer,
                                           CancellationToken cancellationToken = new())
  {
    var wait = WaitForBuffer(cancellationToken);

    if (!wait.IsCompleted)
    {
      return Core();
    }

    wait.WaitSync();
    var count = ReadBuffer(buffer.Span);
    return new ValueTask<int>(count);

    async ValueTask<int> Core()
    {
      await wait.ConfigureAwait(false);

      return ReadBuffer(buffer.Span);
    }
  }

  /// <summary>
  ///   Wait for a new buffer to be ready if the current buffer is empty.
  /// </summary>
  /// <param name="cancellationToken">The token to monitor for cancellation requests.</param>
  /// <returns>A task that represents the asynchronous wait operation.</returns>
  private ValueTask WaitForBuffer(CancellationToken cancellationToken = default)
  {
    // If the current read buffer is not empty or the reader has been closed, we return immediately
    return !currentRead_.IsEmpty || reader_.Completion.IsCompleted
             ? new ValueTask()
             : Core(this,
                    cancellationToken);

    // Asynchronous implementation of the method
    static async ValueTask Core(ChannelStream     self,
                                CancellationToken cancellationToken)
    {
      try
      {
        while (self.currentRead_.IsEmpty)
        {
          self.currentRead_ = await self.reader_.ReadAsync(cancellationToken)
                                        .ConfigureAwait(false);
        }
      }
      catch (ChannelClosedException)
      {
        // Empty on purpose
      }
    }
  }

  /// <summary>
  ///   Reads the current read buffer and write as much as possible into the provided <paramref name="buffer" />.
  /// </summary>
  /// <param name="buffer">Where the read bytes should be written to.</param>
  /// <returns>The number of bytes read.</returns>
  private int ReadBuffer(Span<byte> buffer)
  {
    var read = 0;

    while (!buffer.IsEmpty && !currentRead_.IsEmpty)
    {
      // Current read buffer is smaller than target buffer
      if (currentRead_.Length < buffer.Length)
      {
        read += currentRead_.Length;

        currentRead_.Span.CopyTo(buffer);
        buffer       = buffer[currentRead_.Length..];
        currentRead_ = ReadOnlyMemory<byte>.Empty;

        // Get the next read buffer if it is synchronously available
        if (reader_.TryRead(out var remaining))
        {
          currentRead_ = remaining;
        }
      }
      else
      {
        // Fill the target buffer from the current read buffer
        read += buffer.Length;

        currentRead_.Span[..buffer.Length]
                    .CopyTo(buffer);
        currentRead_ = currentRead_[buffer.Length..];
        buffer       = Span<byte>.Empty;
      }
    }

    return read;
  }

  /// <inheritdoc />
  public override long Seek(long       offset,
                            SeekOrigin origin)
    => throw new NotSupportedException();

  /// <inheritdoc />
  public override void SetLength(long value)
    => throw new NotSupportedException();

  /// <inheritdoc />
  public override void Write(byte[] buffer,
                             int    offset,
                             int    count)
    => Write(buffer.AsSpan(offset,
                           count));

  /// <inheritdoc />
  public override void Write(ReadOnlySpan<byte> buffer)
    => writer_.WriteAsync(buffer.ToArray())
              .WaitSync();

  /// <inheritdoc />
  public override Task WriteAsync(byte[]            buffer,
                                  int               offset,
                                  int               count,
                                  CancellationToken cancellationToken)
    => WriteAsync(buffer.AsMemory(offset,
                                  count),
                  cancellationToken)
      .AsTask();

  /// <inheritdoc />
  public override void Close()
    => writer_.Complete();

  /// <inheritdoc />
  public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer,
                                       CancellationToken    cancellationToken = new())
    => writer_.WriteAsync(buffer.ToArray(),
                          cancellationToken);

  /// <summary>
  ///   Creates a pair of <see cref="ChannelStream" /> that are bound together.
  ///   Everything written into one can be read from the other.
  /// </summary>
  /// <param name="capacity">Number of writes that is buffered by the underlying <see cref="Channel{T}" />.</param>
  /// <returns>The pair of stream.</returns>
  public static (ChannelStream, ChannelStream) CreatePair(int capacity = 0)
  {
    var chan1 = capacity <= 0
                  ? Channel.CreateUnbounded<byte[]>()
                  : Channel.CreateBounded<byte[]>(capacity);
    var chan2 = capacity <= 0
                  ? Channel.CreateUnbounded<byte[]>()
                  : Channel.CreateBounded<byte[]>(capacity);

    var stream1 = new ChannelStream(chan1.Reader,
                                    chan2.Writer);
    var stream2 = new ChannelStream(chan2.Reader,
                                    chan1.Writer);

    return (stream1, stream2);
  }
}
