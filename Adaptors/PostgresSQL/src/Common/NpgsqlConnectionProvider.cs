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
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgresSQL.Options;
using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Injection.Options.Database;

using DbUp;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Npgsql;
using Npgsql.Replication;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Provides NpgsqlDataSource management, schema initialization, and health check
/// </summary>
public class NpgsqlConnectionProvider : IInitializable, IDisposable
{
  private readonly InitDatabase                      initDatabase_;
  private readonly ILogger<NpgsqlConnectionProvider> logger_;
  private readonly PostgreSQL                        options_;
  private          bool                              isInitialized_;

#pragma warning disable CS0618 // Type or member is obsolete
  static NpgsqlConnectionProvider()
    => AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior",
                            true);
#pragma warning restore CS0618

  /// <summary>
  ///   Creates a new NpgsqlConnectionProvider
  /// </summary>
  /// <param name="options">PostgreSQL connection options</param>
  /// <param name="initDatabase">Data to seed during initialization</param>
  /// <param name="logger">Logger</param>
  public NpgsqlConnectionProvider(PostgreSQL                        options,
                                  InitDatabase                      initDatabase,
                                  ILogger<NpgsqlConnectionProvider> logger)
  {
    options_      = options;
    initDatabase_ = initDatabase;
    logger_       = logger;

    var connectionString = BuildConnectionString(options);
    var builder          = new NpgsqlDataSourceBuilder(connectionString);
    DataSource = builder.Build();
  }

  /// <summary>
  ///   The Npgsql data source (connection pool)
  /// </summary>
  public NpgsqlDataSource DataSource { get; }

  /// <inheritdoc />
  public void Dispose()
  {
    DataSource.Dispose();
    GC.SuppressFinalize(this);
  }

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    logger_.LogInformation("Initializing PostgreSQL schema");

    await using var connection = await GetConnectionAsync(cancellationToken)
                                   .ConfigureAwait(false);

    // DbUp opens its own connections against the same database, so the advisory lock
    // is held on a separate connection/transaction for the duration of the upgrade to
    // serialise concurrent instances migrating at the same time.
    await using var lockTransaction = await connection.BeginTransactionAsync(cancellationToken)
                                                      .ConfigureAwait(false);

    await using (var lockCmd = connection.CreateCommand())
    {
      lockCmd.Transaction = lockTransaction;
      lockCmd.CommandText = "SELECT pg_advisory_xact_lock(7243658712345678)";
      await lockCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);
    }

    var upgrader = DeployChanges.To.PostgresqlDatabase(BuildConnectionString(options_))
                                .WithScriptsEmbeddedInAssembly(Assembly.GetExecutingAssembly())
                                .WithTransactionPerScript()
                                .LogTo(logger_)
                                .Build();

    var result = upgrader.PerformUpgrade();
    if (!result.Successful)
    {
      throw new InvalidOperationException("PostgreSQL schema migration failed",
                                          result.Error);
    }

    await lockTransaction.CommitAsync(cancellationToken)
                         .ConfigureAwait(false);

    await SeedInitData(connection,
                       cancellationToken)
      .ConfigureAwait(false);

    isInitialized_ = true;
    logger_.LogInformation("PostgreSQL schema initialized successfully");
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
  {
    if (!isInitialized_)
    {
      return Task.FromResult(HealthCheckResult.Unhealthy("Not initialized"));
    }

    if (tag == HealthCheckTag.Liveness)
    {
      return CheckLiveness();
    }

    return Task.FromResult(HealthCheckResult.Healthy());
  }

  /// <summary>
  ///   Creates a new logical replication connection using the same credentials as the regular connection pool.
  ///   The caller is responsible for opening and disposing the connection.
  ///   Requires <c>wal_level = logical</c> on the PostgreSQL server (or <c>rds.logical_replication = 1</c> on AWS
  ///   RDS/Aurora).
  /// </summary>
  public LogicalReplicationConnection CreateReplicationConnection()
    => new(BuildConnectionString(options_));

  /// <summary>
  ///   Get a new connection from the pool
  /// </summary>
  /// <param name="cancellationToken">Cancellation token</param>
  /// <returns>An open NpgsqlConnection</returns>
  public async Task<NpgsqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
  {
    var connection = await DataSource.OpenConnectionAsync(cancellationToken)
                                     .ConfigureAwait(false);
    return connection;
  }

  private async Task<HealthCheckResult> CheckLiveness()
  {
    try
    {
      await using var connection = await DataSource.OpenConnectionAsync()
                                                   .ConfigureAwait(false);
      await using var cmd = connection.CreateCommand();
      cmd.CommandText = "SELECT 1";
      await cmd.ExecuteScalarAsync()
               .ConfigureAwait(false);
      return HealthCheckResult.Healthy();
    }
    catch (Exception ex)
    {
      return HealthCheckResult.Unhealthy("PostgreSQL connection failed",
                                         ex);
    }
  }

  private async Task SeedInitData(NpgsqlConnection  connection,
                                  CancellationToken cancellationToken)
  {
    foreach (var partition in initDatabase_.Partitions)
    {
      await using var cmd = connection.CreateCommand();
      cmd.CommandText = @"
INSERT INTO partitions (partition_id, parent_partition_ids, pod_reserved, pod_max, preemption_pct, priority, pod_configuration)
VALUES (@partition_id, @parent_partition_ids, @pod_reserved, @pod_max, @preemption_pct, @priority, @pod_configuration::jsonb)
ON CONFLICT (partition_id) DO NOTHING";
      cmd.Parameters.AddWithValue("partition_id",
                                  partition.PartitionId);
      cmd.Parameters.AddWithValue("parent_partition_ids",
                                  NpgsqlDbType.Array | NpgsqlDbType.Text,
                                  partition.ParentPartitionIds.ToArray());
      cmd.Parameters.AddWithValue("pod_reserved",
                                  partition.PodReserved);
      cmd.Parameters.AddWithValue("pod_max",
                                  partition.PodMax);
      cmd.Parameters.AddWithValue("preemption_pct",
                                  partition.PreemptionPercentage);
      cmd.Parameters.AddWithValue("priority",
                                  partition.Priority);
      cmd.Parameters.AddWithValue("pod_configuration",
                                  NpgsqlDbType.Text,
                                  partition.PodConfiguration is not null
                                    ? JsonSerializer.Serialize(partition.PodConfiguration.Configuration)
                                    : "{}");
      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
    }

    foreach (var role in initDatabase_.Roles)
    {
      await using var cmd = connection.CreateCommand();
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
      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
    }

    foreach (var user in initDatabase_.Users)
    {
      await using var cmd = connection.CreateCommand();
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
      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
    }

    foreach (var auth in initDatabase_.Auths)
    {
      await using var cmd = connection.CreateCommand();
      cmd.CommandText = @"
INSERT INTO auth_data (auth_id, user_id, cn, fingerprint)
VALUES (@auth_id, @user_id, @cn, @fingerprint)
ON CONFLICT (auth_id) DO NOTHING";
      cmd.Parameters.AddWithValue("auth_id",
                                  auth.AuthId);
      cmd.Parameters.AddWithValue("user_id",
                                  auth.UserId);
      cmd.Parameters.AddWithValue("cn",
                                  auth.Cn);
      cmd.Parameters.AddWithValue("fingerprint",
                                  (object?)auth.Fingerprint ?? DBNull.Value);
      await cmd.ExecuteNonQueryAsync(cancellationToken)
               .ConfigureAwait(false);
    }
  }

  private static string BuildConnectionString(PostgreSQL options)
  {
    NpgsqlConnectionStringBuilder builder;

    if (!string.IsNullOrEmpty(options.ConnectionString))
    {
      builder = new NpgsqlConnectionStringBuilder(options.ConnectionString)
                {
                  MaxPoolSize        = options.MaxPoolSize,
                  IncludeErrorDetail = true,
                };
      return builder.ConnectionString;
    }

    builder = new NpgsqlConnectionStringBuilder
              {
                Host        = options.Host,
                Port        = options.Port,
                Database    = options.DatabaseName,
                MaxPoolSize = options.MaxPoolSize,
                SslMode = options.Ssl
                            ? SslMode.Require
                            : SslMode.Prefer,
                IncludeErrorDetail = true,
              };

    if (!string.IsNullOrEmpty(options.User))
    {
      builder.Username = options.User;
    }

    if (!string.IsNullOrEmpty(options.Password))
    {
      builder.Password = options.Password;
    }

    return builder.ConnectionString;
  }
}
