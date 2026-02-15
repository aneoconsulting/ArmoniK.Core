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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Common;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Auth.Authentication;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL;

/// <inheritdoc cref="IAuthenticationTable" />
public class AuthenticationTable : IAuthenticationTable
{
  private readonly NpgsqlConnectionProvider connectionProvider_;

  /// <summary>
  ///   Creates a new AuthenticationTable
  /// </summary>
  public AuthenticationTable(NpgsqlConnectionProvider connectionProvider)
    => connectionProvider_ = connectionProvider;

  /// <inheritdoc />
  public async Task<UserAuthenticationResult?> GetIdentityFromCertificateAsync(string            cn,
                                                                               string            fingerprint,
                                                                               CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    // Find the auth entry matching CN and fingerprint (prefer exact fingerprint match, fallback to null fingerprint)
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = @"
SELECT ud.user_id, ud.username, ud.roles
FROM auth_data ad
JOIN user_data ud ON ud.user_id = ad.user_id
WHERE ad.cn = @cn AND (ad.fingerprint = @fingerprint OR ad.fingerprint IS NULL)
ORDER BY ad.fingerprint DESC NULLS LAST
LIMIT 1";
    cmd.Parameters.AddWithValue("cn",
                                cn);
    cmd.Parameters.AddWithValue("fingerprint",
                                fingerprint);

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      return null;
    }

    var userId   = reader.GetInt32(0);
    var username = reader.GetString(1);
    var roleIds  = reader.GetFieldValue<int[]>(2);
    await reader.CloseAsync()
                .ConfigureAwait(false);

    return await BuildAuthenticationResult(connection,
                                           userId,
                                           username,
                                           roleIds,
                                           cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public async Task<UserAuthenticationResult?> GetIdentityFromUserAsync(int?              id,
                                                                        string?           username,
                                                                        CancellationToken cancellationToken = default)
  {
    await using var connection = await connectionProvider_.GetConnectionAsync(cancellationToken)
                                                          .ConfigureAwait(false);

    await using var cmd = connection.CreateCommand();
    if (id.HasValue)
    {
      cmd.CommandText = "SELECT user_id, username, roles FROM user_data WHERE user_id = @user_id";
      cmd.Parameters.AddWithValue("user_id",
                                  id.Value);
    }
    else if (!string.IsNullOrEmpty(username))
    {
      cmd.CommandText = "SELECT user_id, username, roles FROM user_data WHERE username = @username";
      cmd.Parameters.AddWithValue("username",
                                  username);
    }
    else
    {
      return null;
    }

    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken)
                                      .ConfigureAwait(false);

    if (!await reader.ReadAsync(cancellationToken)
                     .ConfigureAwait(false))
    {
      return null;
    }

    var userId       = reader.GetInt32(0);
    var foundName    = reader.GetString(1);
    var roleIds      = reader.GetFieldValue<int[]>(2);
    await reader.CloseAsync()
                .ConfigureAwait(false);

    return await BuildAuthenticationResult(connection,
                                           userId,
                                           foundName,
                                           roleIds,
                                           cancellationToken)
             .ConfigureAwait(false);
  }

  /// <inheritdoc />
  public void AddRoles(IEnumerable<RoleData> roles)
  {
    using var connection = connectionProvider_.DataSource.OpenConnectionAsync()
                                              .AsTask()
                                              .GetAwaiter()
                                              .GetResult();

    foreach (var role in roles)
    {
      using var cmd = connection.CreateCommand();
      cmd.CommandText = @"
INSERT INTO role_data (role_id, role_name, permissions)
VALUES (@role_id, @role_name, @permissions)
ON CONFLICT (role_id) DO NOTHING";
      cmd.Parameters.AddWithValue("role_id",
                                  role.RoleId);
      cmd.Parameters.AddWithValue("role_name",
                                  role.RoleName);
      cmd.Parameters.AddWithValue("permissions",
                                  NpgsqlDbType.Array | NpgsqlDbType.Text,
                                  role.Permissions);
      cmd.ExecuteNonQuery();
    }
  }

  /// <inheritdoc />
  public void AddUsers(IEnumerable<UserData> users)
  {
    using var connection = connectionProvider_.DataSource.OpenConnectionAsync()
                                              .AsTask()
                                              .GetAwaiter()
                                              .GetResult();

    foreach (var user in users)
    {
      using var cmd = connection.CreateCommand();
      cmd.CommandText = @"
INSERT INTO user_data (user_id, username, roles)
VALUES (@user_id, @username, @roles)
ON CONFLICT (user_id) DO NOTHING";
      cmd.Parameters.AddWithValue("user_id",
                                  user.UserId);
      cmd.Parameters.AddWithValue("username",
                                  user.Username);
      cmd.Parameters.AddWithValue("roles",
                                  NpgsqlDbType.Array | NpgsqlDbType.Integer,
                                  user.Roles);
      cmd.ExecuteNonQuery();
    }
  }

  /// <inheritdoc />
  public void AddCertificates(IEnumerable<AuthData> certificates)
  {
    using var connection = connectionProvider_.DataSource.OpenConnectionAsync()
                                              .AsTask()
                                              .GetAwaiter()
                                              .GetResult();

    foreach (var cert in certificates)
    {
      using var cmd = connection.CreateCommand();
      cmd.CommandText = @"
INSERT INTO auth_data (auth_id, user_id, cn, fingerprint)
VALUES (@auth_id, @user_id, @cn, @fingerprint)
ON CONFLICT (auth_id) DO NOTHING";
      cmd.Parameters.AddWithValue("auth_id",
                                  cert.AuthId);
      cmd.Parameters.AddWithValue("user_id",
                                  cert.UserId);
      cmd.Parameters.AddWithValue("cn",
                                  cert.Cn);
      cmd.Parameters.AddWithValue("fingerprint",
                                  (object?)cert.Fingerprint ?? DBNull.Value);
      cmd.ExecuteNonQuery();
    }
  }

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
    => connectionProvider_.Init(cancellationToken);

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => connectionProvider_.Check(tag);

  private static async Task<UserAuthenticationResult> BuildAuthenticationResult(NpgsqlConnection  connection,
                                                                                int               userId,
                                                                                string            username,
                                                                                int[]             roleIds,
                                                                                CancellationToken cancellationToken)
  {
    if (roleIds.Length == 0)
    {
      return new UserAuthenticationResult(userId,
                                          username,
                                          Array.Empty<string>(),
                                          Array.Empty<string>());
    }

    // Get role names and permissions
    await using var roleCmd = connection.CreateCommand();
    roleCmd.CommandText = "SELECT role_name, permissions FROM role_data WHERE role_id = ANY(@role_ids)";
    roleCmd.Parameters.AddWithValue("role_ids",
                                    NpgsqlDbType.Array | NpgsqlDbType.Integer,
                                    roleIds);

    await using var roleReader = await roleCmd.ExecuteReaderAsync(cancellationToken)
                                              .ConfigureAwait(false);

    var roleNames   = new List<string>();
    var permissions = new HashSet<string>();
    while (await roleReader.ReadAsync(cancellationToken)
                           .ConfigureAwait(false))
    {
      roleNames.Add(roleReader.GetString(0));
      var perms = roleReader.GetFieldValue<string[]>(1);
      foreach (var p in perms)
      {
        permissions.Add(p);
      }
    }

    return new UserAuthenticationResult(userId,
                                        username,
                                        roleNames,
                                        permissions);
  }
}
