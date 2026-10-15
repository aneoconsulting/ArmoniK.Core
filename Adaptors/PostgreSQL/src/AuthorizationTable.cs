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

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgreSQL;

/// <inheritdoc />
public class AuthorizationTable : IAuthorizationTable
{
  // Key of the advisory lock serializing changes to grants and permission sets
  private const long LockKey = 0x41726d6f6e694b; // "ArmoniK"

  private readonly ActivitySource           activitySource_;
  private readonly NpgsqlConnectionProvider connectionProvider_;
  private          bool                     isInitialized_;

  /// <summary>
  ///   Creates a new AuthorizationTable
  /// </summary>
  public AuthorizationTable(NpgsqlConnectionProvider connectionProvider,
                            ActivitySource           activitySource)
  {
    connectionProvider_ = connectionProvider;
    activitySource_     = activitySource;
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT * FROM grants";

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return new GrantData(reader.GetString(reader.GetOrdinal("grant_id")),
                                 reader.GetString(reader.GetOrdinal("principal")),
                                 reader.GetString(reader.GetOrdinal("permission_set")),
                                 reader.IsDBNull(reader.GetOrdinal("condition"))
                                   ? null
                                   : reader.GetString(reader.GetOrdinal("condition")),
                                 (AuthorizationOrigin)reader.GetInt32(reader.GetOrdinal("origin")),
                                 DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("creation_date")),
                                                      DateTimeKind.Utc));
    }
  }

  /// <inheritdoc />
  public async Task CreateGrantAsync(GrantData         grant,
                                     CancellationToken cancellationToken = default)
  {
    using var activity = activitySource_.StartActivity();
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    // The advisory lock is held by the session, so the connection is kept until the lock is released
    var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                              .ConfigureAwait(false);
    try
    {
      await using var cmd = connection.CreateCommand();
      cmd.CommandText = "SELECT pg_advisory_lock(@key)";
      cmd.Parameters.AddWithValue("key",
                                  LockKey);
      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
      return new Release(connection);
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
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
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;
    parameters?.Invoke(cmd);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);
    while (await reader.ReadAsync(cancellationToken)
                       .ConfigureAwait(false))
    {
      yield return new PermissionSetData(reader.GetString(reader.GetOrdinal("name")),
                                         reader.GetInt32(reader.GetOrdinal("version")),
                                         reader.GetFieldValue<string[]>(reader.GetOrdinal("permissions")),
                                         (AuthorizationOrigin)reader.GetInt32(reader.GetOrdinal("origin")));
    }
  }

  private sealed class Release : IAuthorizationLock
  {
    private readonly NpgsqlConnection connection_;

    public Release(NpgsqlConnection connection)
      => connection_ = connection;

    // Not reported: the advisory lock lasts as long as the connection holding it
    public CancellationToken Lost
      => CancellationToken.None;

    public async ValueTask DisposeAsync()
    {
      try
      {
        await using var cmd = connection_.CreateCommand();
        cmd.CommandText = "SELECT pg_advisory_unlock(@key)";
        cmd.Parameters.AddWithValue("key",
                                    LockKey);
        await cmd.ExecuteNonQueryAsync()
                 .ConfigureAwait(false);
      }
      finally
      {
        await connection_.DisposeAsync()
                         .ConfigureAwait(false);
      }
    }
  }
}
