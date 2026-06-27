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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="IPartitionTable" />
public class PartitionTable : IPartitionTable
{
  private readonly NpgsqlConnectionProvider connectionProvider_;

  /// <summary>
  ///   Creates a new PartitionTable
  /// </summary>
  public PartitionTable(NpgsqlConnectionProvider connectionProvider)
    => connectionProvider_ = connectionProvider;

  /// <inheritdoc />
  public async Task CreatePartitionsAsync(IEnumerable<PartitionData> partitions,
                                          CancellationToken          cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    await using var batch = new NpgsqlBatch(connection,
                                            transaction);

    foreach (var partition in partitions)
    {
      var cmd = new NpgsqlBatchCommand(@"
INSERT INTO partitions (
  partition_id, parent_partition_ids, pod_reserved, pod_max,
  preemption_pct, priority, pod_configuration
) VALUES (
  @partition_id, @parent_partition_ids, @pod_reserved, @pod_max,
  @preemption_pct, @priority, @pod_configuration
)");
      AddPartitionInsertParameters(cmd.Parameters,
                                   partition);
      batch.BatchCommands.Add(cmd);
    }

    await batch.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);

    await transaction.CommitAsync(cancellationToken)
                     .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<PartitionData> ReadPartitionAsync(string            partitionId,
                                                      CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM partitions WHERE partition_id = @partition_id";
    cmd.Parameters.AddWithValue("partition_id",
                                partitionId);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");
    }

    return RowMapper.MapToPartitionData(reader);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<PartitionData> GetPartitionWithAllocationAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM partitions WHERE pod_max > 0";

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return RowMapper.MapToPartitionData(reader);
    }
  }

  /// <inheritdoc />
  public async Task DeletePartitionAsync(string            partitionId,
                                         CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "DELETE FROM partitions WHERE partition_id = @partition_id";
    cmd.Parameters.AddWithValue("partition_id",
                                partitionId);

    var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken)
                           .ConfigureAwait(false);

    if (deleted == 0)
    {
      throw new PartitionNotFoundException($"Partition '{partitionId}' not found.");
    }
  }

  /// <inheritdoc />
  public async Task<bool> ArePartitionsExistingAsync(IEnumerable<string> partitionIds,
                                                     CancellationToken   cancellationToken = default)
  {
    var ids = partitionIds.ToArray();

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT COUNT(*) FROM partitions WHERE partition_id = ANY(@ids)";
    cmd.Parameters.AddWithValue("ids",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                ids);

    var count = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken)
                                         .ConfigureAwait(false));

    return count == ids.Length;
  }

  /// <inheritdoc />
  public async Task<(IEnumerable<PartitionData> partitions, int totalCount)> ListPartitionsAsync(Expression<Func<PartitionData, bool>>    filter,
                                                                                                  Expression<Func<PartitionData, object?>> orderField,
                                                                                                  bool                                     ascOrder,
                                                                                                  int                                      page,
                                                                                                  int                                      pageSize,
                                                                                                  CancellationToken                        cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<PartitionData>.Translate(filter);
    var orderColumn             = ExpressionToSql<PartitionData>.TranslateOrderBy(orderField);
    var orderDir                = ascOrder ? "ASC" : "DESC";

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Count query
    await using var countCmd = connection.CreateCommand();
    countCmd.CommandText = $"SELECT COUNT(*) FROM partitions WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(countCmd,
                                      whereParams);
    var totalCount = Convert.ToInt32(await countCmd.ExecuteScalarAsync(cancellationToken)
                                                   .ConfigureAwait(false));

    if (pageSize <= 0)
    {
      return (Enumerable.Empty<PartitionData>(), totalCount);
    }

    // Data query
    await using var dataCmd = connection.CreateCommand();
    dataCmd.CommandText = $"SELECT * FROM partitions WHERE {whereSql} ORDER BY {orderColumn} {orderDir} LIMIT @limit OFFSET @offset";
    SqlHelper.AddExpressionParameters(dataCmd,
                                      whereParams);
    dataCmd.Parameters.AddWithValue("limit",
                                    pageSize);
    dataCmd.Parameters.AddWithValue("offset",
                                    page * pageSize);

    await using var reader = await dataCmd.ExecuteReaderAsync(cancellationToken)
                                          .ConfigureAwait(false);

    var results = new List<PartitionData>();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      results.Add(RowMapper.MapToPartitionData(reader));
    }

    return (results, totalCount);
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<T> FindPartitionsAsync<T>(Expression<Func<PartitionData, bool>>         filter,
                                                           Expression<Func<PartitionData, T>>             selector,
                                                           [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var (whereSql, whereParams) = ExpressionToSql<PartitionData>.Translate(filter);

    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = $"SELECT * FROM partitions WHERE {whereSql}";
    SqlHelper.AddExpressionParameters(cmd,
                                      whereParams);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    var compiled = selector.Compile();
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return compiled.Invoke(RowMapper.MapToPartitionData(reader));
    }
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private static void AddPartitionInsertParameters(NpgsqlParameterCollection parameters,
                                                   PartitionData             partition)
  {
    parameters.AddWithValue("partition_id",
                            partition.PartitionId);
    parameters.AddWithValue("parent_partition_ids",
                            NpgsqlDbType.Array | NpgsqlDbType.Text,
                            partition.ParentPartitionIds.ToArray());
    parameters.AddWithValue("pod_reserved",
                            partition.PodReserved);
    parameters.AddWithValue("pod_max",
                            partition.PodMax);
    parameters.AddWithValue("preemption_pct",
                            partition.PreemptionPercentage);
    parameters.AddWithValue("priority",
                            partition.Priority);

    if (partition.PodConfiguration is not null)
    {
      parameters.AddWithValue("pod_configuration",
                              NpgsqlDbType.Jsonb,
                              JsonSerializer.Serialize(partition.PodConfiguration.Configuration));
    }
    else
    {
      parameters.AddWithValue("pod_configuration",
                              NpgsqlDbType.Jsonb,
                              DBNull.Value);
    }
  }
}
