// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.TaskDB.Options;

using MessagePack;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.TaskDB.Protocol;

/// <summary>
///   Thread-safe TCP client for the TaskDB binary protocol.
///   Frame format (request):  [4-byte length BE][1-byte opcode][payload]
///   Frame format (response): [4-byte length BE][1-byte status][payload]
/// </summary>
public sealed class TaskDbConnection : IDisposable
{
  private readonly ILogger                  logger_;
  private readonly Options.TaskDB           options_;
  private          TcpClient?               tcp_;
  private          NetworkStream?           stream_;
  // One lock per connection — fine for PoC; replace with channel/pool for production.
  private readonly SemaphoreSlim            semaphore_ = new(1, 1);

  public TaskDbConnection(Options.TaskDB options,
                          ILogger        logger)
  {
    options_ = options;
    logger_  = logger;
  }

  // ── Connection management ────────────────────────────────────────────────

  public async Task ConnectAsync(CancellationToken cancellationToken)
  {
    tcp_    = new TcpClient();
    tcp_.ReceiveTimeout = (int)options_.SocketTimeout.TotalMilliseconds;
    tcp_.SendTimeout    = (int)options_.SocketTimeout.TotalMilliseconds;
    await tcp_.ConnectAsync(options_.Host,
                            options_.Port,
                            cancellationToken)
              .ConfigureAwait(false);
    stream_ = tcp_.GetStream();
    logger_.LogInformation("Connected to TaskDB at {host}:{port}",
                           options_.Host,
                           options_.Port);
  }

  public void Dispose()
  {
    stream_?.Dispose();
    tcp_?.Dispose();
    semaphore_.Dispose();
  }

  // ── Low-level frame I/O ──────────────────────────────────────────────────

  private async Task SendAsync(byte opCode, byte[] payload, CancellationToken ct)
  {
    var header = new byte[5];
    BinaryPrimitives.WriteUInt32BigEndian(header, (uint)(payload.Length + 1));
    header[4] = opCode;

    var ns = stream_ ?? throw new InvalidOperationException("Not connected");
    await ns.WriteAsync(header, ct).ConfigureAwait(false);
    if (payload.Length > 0)
      await ns.WriteAsync(payload, ct).ConfigureAwait(false);
  }

  private async Task<(byte status, byte[] payload)> ReceiveAsync(CancellationToken ct)
  {
    var ns     = stream_ ?? throw new InvalidOperationException("Not connected");
    var header = new byte[5];
    await ReadExactAsync(ns, header, ct).ConfigureAwait(false);

    var status = header[4];
    var length = BinaryPrimitives.ReadUInt32BigEndian(header);
    var payloadLen = (int)length - 1;

    if (payloadLen <= 0)
      return (status, Array.Empty<byte>());

    var payload = new byte[payloadLen];
    await ReadExactAsync(ns, payload, ct).ConfigureAwait(false);
    return (status, payload);
  }

  private static async Task ReadExactAsync(Stream stream, byte[] buffer, CancellationToken ct)
  {
    var offset = 0;
    while (offset < buffer.Length)
    {
      var read = await stream.ReadAsync(buffer.AsMemory(offset), ct).ConfigureAwait(false);
      if (read == 0)
        throw new EndOfStreamException("TaskDB connection closed unexpectedly");
      offset += read;
    }
  }

  // ── Public RPC helpers ───────────────────────────────────────────────────

  /// <summary>
  ///   Send a request and receive a single response frame.
  /// </summary>
  public async Task<(byte status, byte[] payload)> SendReceiveAsync<TReq>(byte              opCode,
                                                                           TReq              request,
                                                                           CancellationToken ct)
  {
    var encoded = MessagePackSerializer.Serialize(request, cancellationToken: ct);
    await semaphore_.WaitAsync(ct).ConfigureAwait(false);
    try
    {
      await SendAsync(opCode, encoded, ct).ConfigureAwait(false);
      return await ReceiveAsync(ct).ConfigureAwait(false);
    }
    finally
    {
      semaphore_.Release();
    }
  }

  /// <summary>
  ///   Send a request and read all streaming response frames until StreamEnd.
  /// </summary>
  public async IAsyncEnumerable<byte[]> StreamAsync<TReq>(byte              opCode,
                                                           TReq              request,
                                                           [System.Runtime.CompilerServices.EnumeratorCancellation]
                                                           CancellationToken ct)
  {
    var encoded = MessagePackSerializer.Serialize(request, cancellationToken: ct);
    await semaphore_.WaitAsync(ct).ConfigureAwait(false);
    try
    {
      await SendAsync(opCode, encoded, ct).ConfigureAwait(false);

      while (true)
      {
        var (status, payload) = await ReceiveAsync(ct).ConfigureAwait(false);

        if (status == StatusCode.StreamEnd)
          yield break;

        if (status == StatusCode.ServerError)
          throw new IOException($"TaskDB server error during streaming op 0x{opCode:X2}");

        yield return payload;
      }
    }
    finally
    {
      semaphore_.Release();
    }
  }

  // ── Health check ─────────────────────────────────────────────────────────

  public async Task<bool> PingAsync(CancellationToken ct)
  {
    try
    {
      var (status, _) = await SendReceiveAsync(OpCode.Ping, new { }, ct).ConfigureAwait(false);
      return status == StatusCode.Success;
    }
    catch
    {
      return false;
    }
  }
}
