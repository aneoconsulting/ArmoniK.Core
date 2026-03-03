// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.TaskDB.Protocol;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using MessagePack;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.TaskDB;

/// <inheritdoc cref="IPartitionTable" />
public class PartitionTable : IPartitionTable
{
  private readonly TaskDbConnection        connection_;
  private readonly ActivitySource          activitySource_;
  private readonly ILogger<PartitionTable> logger_;

  public PartitionTable(TaskDbConnection        connection,
                        ActivitySource          activitySource,
                        ILogger<PartitionTable> logger)
  {
    connection_     = connection;
    activitySource_ = activitySource;
    logger_         = logger;
  }

  /// <inheritdoc />
  public ILogger Logger => logger_;

  // ── IInitializable ───────────────────────────────────────────────────────

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(HealthCheckResult.Healthy());

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => Task.CompletedTask;

  // ── IPartitionTable ──────────────────────────────────────────────────────

  /// <inheritdoc />
  public async Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                                           CancellationToken          cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireCreatePartitionsRequest
    {
      Partitions = partitions.Select(p => p.ToWire()).ToList(),
    };

    var (status, _) = await connection_.SendReceiveAsync(OpCode.CreatePartitions, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"CreatePartitions failed with status 0x{status:X2}");
  }

  /// <inheritdoc />
  public async Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                                       CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("ReadPartitionId", partitionId);

    var req = new WireReadPartitionRequest { PartitionId = partitionId };
    var (status, payload) = await connection_.SendReceiveAsync(OpCode.ReadPartition, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status == StatusCode.NotFound)
      throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ReadPartition failed with status 0x{status:X2}");

    return MessagePackSerializer.Deserialize<WirePartitionData>(payload).ToDomain();
  }

  /// <inheritdoc />
  public async Task<bool> ArePartitionsExistingAsync(IEnumerable<string> partitionIds,
                                                      CancellationToken   cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var ids = partitionIds.ToList();
    var req = new WireArePartitionsExistingRequest { PartitionIds = ids };
    var (status, payload) = await connection_.SendReceiveAsync(OpCode.ArePartitionsExisting, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ArePartitionsExisting failed with status 0x{status:X2}");

    var resp = MessagePackSerializer.Deserialize<WireArePartitionsExistingResponse>(payload);
    return resp.Exists.Values.All(v => v);
  }

  /// <inheritdoc />
  // IPartitionTable.GetPartitionWithAllocationAsync takes no partition IDs —
  // it returns ALL partitions where PodMax > 0.
  public async IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync(
    [EnumeratorCancellation]
    CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireGetPartitionsWithAllocationRequest();

    await foreach (var frame in connection_.StreamAsync(OpCode.GetPartitionsWithAllocation, req, cancellationToken)
                                           .ConfigureAwait(false))
    {
      yield return MessagePackSerializer.Deserialize<WirePartitionData>(frame).ToDomain();
    }
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<PartitionData> partitions, int totalCount)> ListPartitionsAsync(
    Expression<Func<PartitionData, bool>>    filter,
    Expression<Func<PartitionData, object?>> orderField,
    bool                                     ascOrder,
    int                                      page,
    int                                      pageSize,
    CancellationToken                        cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();

    var req = new WireFindPartitionsRequest
    {
      Filter    = new WireCoarsePartitionFilter(),
      OrderBy   = ExtractFieldName(orderField),
      Ascending = ascOrder,
      Page      = page,
      PageSize  = pageSize,
    };

    var (status, payload) = await connection_.SendReceiveAsync(OpCode.ListPartitions, req, cancellationToken)
                                             .ConfigureAwait(false);

    if (status != StatusCode.Success)
      throw new ArmoniKException($"ListPartitions failed with status 0x{status:X2}");

    var resp           = MessagePackSerializer.Deserialize<WireListPartitionsResponse>(payload);
    var compiledFilter = filter.Compile();

    var partitions = resp.Partitions
                         .Select(w => w.ToDomain())
                         .Where(compiledFilter);

    return (partitions, (int)resp.TotalCount);
  }

  /// <inheritdoc />
  // IPartitionTable.FindPartitionsAsync<T> takes a filter AND a selector expression
  public IAsyncEnumerable<T> FindPartitionsAsync<T>(Expression<Func<PartitionData, bool>> filter,
                                                     Expression<Func<PartitionData, T>>    selector,
                                                     CancellationToken                     cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    return FindPartitionsInternalAsync(filter, selector, cancellationToken);
  }

  private async IAsyncEnumerable<T> FindPartitionsInternalAsync<T>(
    Expression<Func<PartitionData, bool>> filter,
    Expression<Func<PartitionData, T>>    selector,
    [EnumeratorCancellation]
    CancellationToken                     cancellationToken)
  {
    var req = new WireFindPartitionsRequest
    {
      Filter    = new WireCoarsePartitionFilter(),
      Ascending = true,
      PageSize  = 0,
    };

    var compiledFilter   = filter.Compile();
    var compiledSelector = selector.Compile();

    await foreach (var frame in connection_.StreamAsync(OpCode.FindPartitions, req, cancellationToken)
                                           .ConfigureAwait(false))
    {
      var partition = MessagePackSerializer.Deserialize<WirePartitionData>(frame).ToDomain();
      if (compiledFilter(partition))
        yield return compiledSelector(partition);
    }
  }

  /// <inheritdoc />
  public async Task DeletePartitionAsync(string            partitionId,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    activity?.SetTag("DeletePartitionId", partitionId);

    var req = new WireDeletePartitionRequest { PartitionId = partitionId };
    var (status, _) = await connection_.SendReceiveAsync(OpCode.DeletePartition, req, cancellationToken)
                                       .ConfigureAwait(false);

    if (status != StatusCode.Success && status != StatusCode.NotFound)
      throw new ArmoniKException($"DeletePartition failed with status 0x{status:X2}");
  }

  private static string ExtractFieldName<T>(Expression<Func<T, object?>> expr)
  {
    var body = (Expression)expr.Body;
    if (body is UnaryExpression u) body = u.Operand;
    return body is MemberExpression m ? m.Member.Name : "";
  }
}
