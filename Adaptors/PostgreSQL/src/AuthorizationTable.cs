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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgreSQL;

/// <inheritdoc />
public class AuthorizationTable : IAuthorizationTable
{
  // Key of the advisory lock serializing changes to grants and permission sets
  private const long LockKey = 0x41726d6f6e694b; // "ArmoniK"

  // Delay between two attempts to take the lock while another replica holds it
  private static readonly TimeSpan LockRetryDelay = TimeSpan.FromMilliseconds(100);

  private readonly ActivitySource activitySource_;

  // Connection of the transaction holding the lock, for the table given by the lock; null for the pool
  private readonly NpgsqlConnection?           bound_;
  private readonly NpgsqlConnectionProvider    connectionProvider_;
  private readonly ILogger<AuthorizationTable> logger_;
  private          bool                        isInitialized_;

  /// <summary>
  ///   Creates a new AuthorizationTable
  /// </summary>
  public AuthorizationTable(NpgsqlConnectionProvider    connectionProvider,
                            ActivitySource              activitySource,
                            ILogger<AuthorizationTable> logger)
  {
    connectionProvider_ = connectionProvider;
    activitySource_     = activitySource;
    logger_             = logger;
  }

  private AuthorizationTable(AuthorizationTable table,
                             NpgsqlConnection   bound)
  {
    connectionProvider_ = table.connectionProvider_;
    activitySource_     = table.activitySource_;
    logger_             = table.logger_;
    isInitialized_      = true;
    bound_              = bound;
  }

  /// <inheritdoc />
  public async Task<HealthCheckResult> Check(HealthCheckTag tag)
    => isInitialized_
         ? await connectionProvider_.Check(tag)
                                    .ConfigureAwait(false)
         : HealthCheckResult.Unhealthy($"{nameof(AuthorizationTable)} is not initialized");

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    await connectionProvider_.Init(cancellationToken)
                             .ConfigureAwait(false);
    isInitialized_ = true;
  }

  /// <inheritdoc />
  public async IAsyncEnumerable<GrantData> ListGrantsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM grants";

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return RowMapper.MapToGrantData(reader);
    }
  }

  /// <inheritdoc />
  public async Task CreateGrantAsync(GrantData         grant,
                                     CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = @"
INSERT INTO grants (grant_id, principal, permission_set, condition, origin, creation_date)
VALUES (@grant_id, @principal, @permission_set, @condition, @origin, @creation_date)";
    cmd.Parameters.AddWithValue("grant_id",
                                grant.GrantId);
    cmd.Parameters.AddWithValue("principal",
                                grant.Principal);
    cmd.Parameters.AddWithValue("permission_set",
                                grant.PermissionSet);
    cmd.Parameters.AddWithValue("condition",
                                NpgsqlDbType.Text,
                                (object?)grant.Condition ?? DBNull.Value);
    cmd.Parameters.AddWithValue("origin",
                                (int)grant.Origin);
    cmd.Parameters.AddWithValue("creation_date",
                                grant.CreationDate);

    try
    {
      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
    }
    catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.UniqueViolation)
    {
      throw new GrantAlreadyExistsException($"Grant '{grant.GrantId}' already exists",
                                            e);
    }
  }

  /// <inheritdoc />
  public async Task<bool> DeleteGrantAsync(string            grantId,
                                           CancellationToken cancellationToken = default)
    => await ExecuteAsync("DELETE FROM grants WHERE grant_id = @id",
                          ("id", grantId),
                          cancellationToken)
         .ConfigureAwait(false) > 0;

  /// <inheritdoc />
  public IAsyncEnumerable<PermissionSetData> ListPermissionSetsAsync(CancellationToken cancellationToken = default)
    => ReadPermissionSetsAsync("SELECT * FROM permission_sets",
                               null,
                               cancellationToken);

  /// <inheritdoc />
  public async Task<PermissionSetData?> GetPermissionSetVersionAsync(string            name,
                                                                     int               version,
                                                                     CancellationToken cancellationToken = default)
  {
    await foreach (var set in ReadPermissionSetsAsync("SELECT * FROM permission_set_versions WHERE name = @name AND version = @version",
                                                      cmd =>
                                                      {
                                                        cmd.Parameters.AddWithValue("name",
                                                                                    name);
                                                        cmd.Parameters.AddWithValue("version",
                                                                                    version);
                                                      },
                                                      cancellationToken)
                     .ConfigureAwait(false))
    {
      return set;
    }

    return null;
  }

  /// <inheritdoc />
  public async Task<int> GetLatestPermissionSetVersionAsync(string            name,
                                                            CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT COALESCE(MAX(version), 0) FROM permission_set_versions WHERE name = @name";
    cmd.Parameters.AddWithValue("name",
                                name);
    return (int)(await cmd.ExecuteScalarAsync(cancellationToken)
                          .ConfigureAwait(false))!;
  }

  /// <inheritdoc />
  public async Task SetPermissionSetAsync(PermissionSetData permissionSet,
                                          CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);

    // A stored version is never rewritten: written again only with the same content
    await using (var version = connection.CreateCommand())
    {
      version.CommandText = @"
INSERT INTO permission_set_versions (name, version, permissions, origin)
VALUES (@name, @version, @permissions, @origin)
ON CONFLICT (name, version) DO NOTHING";
      AddParameters(version,
                    permissionSet);
      if (await version.ExecuteNonQueryAsync(cancellationToken)
                       .ConfigureAwait(false) == 0)
      {
        var stored = await GetPermissionSetVersionAsync(permissionSet.Name,
                                                        permissionSet.Version,
                                                        cancellationToken)
                       .ConfigureAwait(false);
        if (stored is null || !stored.SameAs(permissionSet))
        {
          throw new PermissionSetVersionAlreadyExistsException($"Permission set {permissionSet.Name}@{permissionSet.Version} is already stored with other permissions");
        }
      }
    }

    await using var current = connection.CreateCommand();
    current.CommandText = @"
INSERT INTO permission_sets (name, version, permissions, origin)
VALUES (@name, @version, @permissions, @origin)
ON CONFLICT (name) DO UPDATE SET version = EXCLUDED.version, permissions = EXCLUDED.permissions, origin = EXCLUDED.origin";
    AddParameters(current,
                  permissionSet);
    await current.ExecuteNonQueryAsync(cancellationToken)
                 .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<bool> DeletePermissionSetAsync(string            name,
                                                   CancellationToken cancellationToken = default)
    => await ExecuteAsync("DELETE FROM permission_sets WHERE name = @id",
                          ("id", name),
                          cancellationToken)
         .ConfigureAwait(false) > 0;

  /// <inheritdoc />
  public async Task<int> GetConfigRevisionAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT revision FROM authorization_state WHERE id = 0";

    return await cmd.ExecuteScalarAsync(cancellationToken)
                    .ConfigureAwait(false) is int revision
             ? revision
             : 0;
  }

  /// <inheritdoc />
  public async Task SetConfigRevisionAsync(int               revision,
                                           CancellationToken cancellationToken = default)
    => await ExecuteAsync(@"
INSERT INTO authorization_state (id, revision) VALUES (0, @id)
ON CONFLICT (id) DO UPDATE SET revision = GREATEST(authorization_state.revision, EXCLUDED.revision)",
                          ("id", revision),
                          cancellationToken)
         .ConfigureAwait(false);

  /// <inheritdoc />
  public async Task<IAuthorizationLock> LockAsync(CancellationToken cancellationToken = default)
  {
    // The lock is held by a transaction, in which every write under it is done: the writes are kept only when committed
    // while the lock is held, and the lock is released when the transaction ends however it ends
    var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                              .ConfigureAwait(false);
    try
    {
      while (true)
      {
        var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                          .ConfigureAwait(false);
        await using (var cmd = connection.CreateCommand())
        {
          cmd.CommandText = "SELECT pg_try_advisory_xact_lock(@key)";
          cmd.Parameters.AddWithValue("key",
                                      LockKey);
          if ((bool)(await cmd.ExecuteScalarAsync(cancellationToken)
                              .ConfigureAwait(false))!)
          {
            return new Release(new AuthorizationTable(this,
                                                      connection),
                               connection,
                               transaction);
          }
        }

        // Tried again in a new transaction, which does not keep a snapshot while waiting
        await transaction.DisposeAsync()
                         .ConfigureAwait(false);
        await Task.Delay(LockRetryDelay,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }
    catch
    {
      await connection.DisposeAsync()
                      .ConfigureAwait(false);
      throw;
    }
  }

  /// <inheritdoc />
  public async Task<AuthorizationGeneration> GetGenerationAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT begun, ended FROM authorization_state WHERE id = 0";

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);
    return await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false)
             ? new AuthorizationGeneration(reader.GetInt64(0),
                                           reader.GetInt64(1))
             : default;
  }

  /// <inheritdoc />
  public async Task<long> BeginChangeAsync(CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = @"
INSERT INTO authorization_state (id, begun, ended) VALUES (0, 1, 0)
ON CONFLICT (id) DO UPDATE SET begun = authorization_state.begun + 1
RETURNING begun";

    return (long)(await cmd.ExecuteScalarAsync(cancellationToken)
                           .ConfigureAwait(false))!;
  }

  /// <inheritdoc />
  public async Task EndChangeAsync(long              generation,
                                   CancellationToken cancellationToken = default)
    => await ExecuteAsync("UPDATE authorization_state SET ended = @generation WHERE id = 0 AND begun = @generation",
                          ("generation", generation),
                          cancellationToken)
         .ConfigureAwait(false);

  // A connection of the pool, or the one of the transaction holding the lock, left open when disposed
  private async ValueTask<Connection> OpenAsync(CancellationToken cancellationToken)
    => bound_ is not null
         ? new Connection(bound_,
                          false)
         : new Connection(await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                   .ConfigureAwait(false),
                          true);

  private static void AddParameters(NpgsqlCommand     cmd,
                                    PermissionSetData permissionSet)
  {
    cmd.Parameters.AddWithValue("name",
                                permissionSet.Name);
    cmd.Parameters.AddWithValue("version",
                                permissionSet.Version);
    cmd.Parameters.AddWithValue("permissions",
                                NpgsqlDbType.Array | NpgsqlDbType.Text,
                                permissionSet.Permissions.ToArray());
    cmd.Parameters.AddWithValue("origin",
                                (int)permissionSet.Origin);
  }

  private async Task<int> ExecuteAsync(string            sql,
                                       (string, object)  parameter,
                                       CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;
    cmd.Parameters.AddWithValue(parameter.Item1,
                                parameter.Item2);
    return await cmd.ExecuteNonQueryAsync(cancellationToken)
                    .ConfigureAwait(false);
  }

  private async IAsyncEnumerable<PermissionSetData> ReadPermissionSetsAsync(string                                     sql,
                                                                            Action<NpgsqlCommand>?                     parameters,
                                                                            [EnumeratorCancellation] CancellationToken cancellationToken)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await OpenAsync(cancellationToken)
                                   .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;
    parameters?.Invoke(cmd);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return RowMapper.MapToPermissionSetData(reader);
    }
  }

  private readonly record struct Connection(NpgsqlConnection Inner,
                                            bool             Owned) : IAsyncDisposable
  {
    public ValueTask DisposeAsync()
      => Owned
           ? Inner.DisposeAsync()
           : ValueTask.CompletedTask;

    public NpgsqlCommand CreateCommand()
      => Inner.CreateCommand();
  }

  /// <summary>
  ///   Advisory lock held by a transaction, in which the table it gives writes
  /// </summary>
  private sealed class Release(AuthorizationTable table,
                               NpgsqlConnection   connection,
                               NpgsqlTransaction  transaction) : IAuthorizationLock
  {
    // Never reported lost: a lost lock fails the writes done under it and their commit, and discards them
    public CancellationToken Lost
      => CancellationToken.None;

    public IAuthorizationTable Table
      => table;

    public bool Atomic
      => true;

    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
      // A transaction that failed is rolled back by COMMIT without an error: a statement fails in it beforehand
      await using (var cmd = connection.CreateCommand())
      {
        cmd.CommandText = "SELECT pg_try_advisory_xact_lock(@key)";
        cmd.Parameters.AddWithValue("key",
                                    LockKey);
        if (!(bool)(await cmd.ExecuteScalarAsync(cancellationToken)
                             .ConfigureAwait(false))!)
        {
          throw new InvalidOperationException("The authorization lock is no longer held");
        }
      }

      await transaction.CommitAsync(cancellationToken)
                       .ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
      try
      {
        // Rolled back unless committed, which releases the lock
        await transaction.DisposeAsync()
                         .ConfigureAwait(false);
      }
      finally
      {
        await connection.DisposeAsync()
                        .ConfigureAwait(false);
      }
    }
  }
}
