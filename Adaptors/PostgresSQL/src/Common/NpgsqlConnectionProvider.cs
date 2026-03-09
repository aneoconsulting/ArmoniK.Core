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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Injection.Options.Database;

using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;

using Npgsql;

using NpgsqlTypes;

namespace ArmoniK.Core.Adapters.PostgresSQL.Common;

/// <summary>
///   Provides NpgsqlDataSource management, schema initialization, and health check
/// </summary>
public class NpgsqlConnectionProvider : IInitializable, IDisposable
{
  private readonly InitDatabase                       initDatabase_;
  private readonly ILogger<NpgsqlConnectionProvider> logger_;
  private readonly Options.PostgreSQL                options_;
  private          bool                              isInitialized_;

  /// <summary>
  ///   The Npgsql data source (connection pool)
  /// </summary>
  public NpgsqlDataSource DataSource { get; }

  /// <summary>
  ///   Creates a new NpgsqlConnectionProvider
  /// </summary>
  /// <param name="options">PostgreSQL connection options</param>
  /// <param name="initDatabase">Data to seed during initialization</param>
  /// <param name="logger">Logger</param>
  public NpgsqlConnectionProvider(Options.PostgreSQL                options,
                                  InitDatabase                      initDatabase,
                                  ILogger<NpgsqlConnectionProvider> logger)
  {
    options_      = options;
    initDatabase_ = initDatabase;
    logger_       = logger;

#pragma warning disable CS0618 // Type or member is obsolete
    AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior",
                         true);
#pragma warning restore CS0618

    var connectionString = BuildConnectionString(options);
    var builder          = new NpgsqlDataSourceBuilder(connectionString);
    DataSource = builder.Build();
  }

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

  /// <inheritdoc />
  public async Task Init(CancellationToken cancellationToken)
  {
    logger_.LogInformation("Initializing PostgreSQL schema");

    await using var connection = await GetConnectionAsync(cancellationToken)
                                   .ConfigureAwait(false);

    await using var transaction = await connection.BeginTransactionAsync(cancellationToken)
                                                  .ConfigureAwait(false);

    await using (var lockCmd = connection.CreateCommand())
    {
      lockCmd.Transaction = transaction;
      lockCmd.CommandText = "SELECT pg_advisory_xact_lock(7243658712345678)";
      await lockCmd.ExecuteNonQueryAsync(cancellationToken)
                   .ConfigureAwait(false);
    }

    await using var cmd = connection.CreateCommand();
    cmd.Transaction  = transaction;
    cmd.CommandText = SchemaDdl;

    await cmd.ExecuteNonQueryAsync(cancellationToken)
             .ConfigureAwait(false);

    await transaction.CommitAsync(cancellationToken)
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

  /// <inheritdoc />
  public void Dispose()
  {
    DataSource.Dispose();
    GC.SuppressFinalize(this);
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
                                    ? System.Text.Json.JsonSerializer.Serialize(partition.PodConfiguration.Configuration)
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

  private static string BuildConnectionString(Options.PostgreSQL options)
  {
    if (!string.IsNullOrEmpty(options.ConnectionString))
    {
      return options.ConnectionString;
    }

    var builder = new NpgsqlConnectionStringBuilder
                  {
                    Host            = options.Host,
                    Port            = options.Port,
                    Database        = options.DatabaseName,
                    MaxPoolSize     = options.MaxPoolSize,
                    SslMode         = options.Ssl ? SslMode.Require : SslMode.Prefer,
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

  private const string SchemaDdl = @"
-- Tasks table
CREATE TABLE IF NOT EXISTS tasks (
    session_id          TEXT NOT NULL,
    task_id             TEXT PRIMARY KEY,
    owner_pod_id        TEXT NOT NULL DEFAULT '',
    owner_pod_name      TEXT NOT NULL DEFAULT '',
    payload_id          TEXT NOT NULL,
    parent_task_ids     TEXT[] NOT NULL DEFAULT '{}',
    data_dependencies   TEXT[] NOT NULL DEFAULT '{}',
    expected_output_ids TEXT[] NOT NULL DEFAULT '{}',
    initial_task_id     TEXT NOT NULL,
    created_by          TEXT NOT NULL DEFAULT '',
    retry_of_ids        TEXT[] NOT NULL DEFAULT '{}',
    status              INTEGER NOT NULL,
    status_message      TEXT NOT NULL DEFAULT '',
    options_options          JSONB NOT NULL DEFAULT '{}',
    options_max_duration     BIGINT NOT NULL DEFAULT 0,
    options_max_retries      INTEGER NOT NULL DEFAULT 0,
    options_priority         INTEGER NOT NULL DEFAULT 0,
    options_partition_id     TEXT NOT NULL DEFAULT '',
    options_app_name         TEXT NOT NULL DEFAULT '',
    options_app_version      TEXT NOT NULL DEFAULT '',
    options_app_namespace    TEXT NOT NULL DEFAULT '',
    options_app_service      TEXT NOT NULL DEFAULT '',
    options_engine_type      TEXT NOT NULL DEFAULT '',
    creation_date       TIMESTAMP NOT NULL,
    submitted_date      TIMESTAMP,
    start_date          TIMESTAMP,
    end_date            TIMESTAMP,
    reception_date      TIMESTAMP,
    acquisition_date    TIMESTAMP,
    processed_date      TIMESTAMP,
    fetched_date        TIMESTAMP,
    pod_ttl             TIMESTAMP,
    processing_to_end_duration BIGINT,
    creation_to_end_duration   BIGINT,
    received_to_end_duration   BIGINT,
    output_status       INTEGER NOT NULL DEFAULT 0,
    output_error        TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_tasks_session_id ON tasks(session_id);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_partition_status ON tasks(options_partition_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_owner_pod_id ON tasks(owner_pod_id);
CREATE INDEX IF NOT EXISTS idx_tasks_initial_task_id ON tasks(initial_task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_created_by ON tasks(created_by);
CREATE INDEX IF NOT EXISTS idx_tasks_creation_date ON tasks(creation_date);

-- Association table for RemainingDataDependencies
CREATE TABLE IF NOT EXISTS task_remaining_dependencies (
    task_id       TEXT NOT NULL REFERENCES tasks(task_id) ON DELETE CASCADE,
    dependency_id TEXT NOT NULL,
    PRIMARY KEY (task_id, dependency_id)
);

CREATE INDEX IF NOT EXISTS idx_trd_dependency ON task_remaining_dependencies(dependency_id);

-- Sessions table
CREATE TABLE IF NOT EXISTS sessions (
    session_id        TEXT PRIMARY KEY,
    status            INTEGER NOT NULL,
    client_submission BOOLEAN NOT NULL DEFAULT TRUE,
    worker_submission BOOLEAN NOT NULL DEFAULT TRUE,
    creation_date     TIMESTAMP NOT NULL,
    cancellation_date TIMESTAMP,
    closure_date      TIMESTAMP,
    purge_date        TIMESTAMP,
    deletion_date     TIMESTAMP,
    deletion_ttl      TIMESTAMP,
    duration          BIGINT,
    partition_ids     TEXT[] NOT NULL DEFAULT '{}',
    options_options          JSONB NOT NULL DEFAULT '{}',
    options_max_duration     BIGINT NOT NULL DEFAULT 0,
    options_max_retries      INTEGER NOT NULL DEFAULT 0,
    options_priority         INTEGER NOT NULL DEFAULT 0,
    options_partition_id     TEXT NOT NULL DEFAULT '',
    options_app_name         TEXT NOT NULL DEFAULT '',
    options_app_version      TEXT NOT NULL DEFAULT '',
    options_app_namespace    TEXT NOT NULL DEFAULT '',
    options_app_service      TEXT NOT NULL DEFAULT '',
    options_engine_type      TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sessions_creation_date ON sessions(creation_date);

-- Results table
CREATE TABLE IF NOT EXISTS results (
    session_id      TEXT NOT NULL,
    result_id       TEXT PRIMARY KEY,
    name            TEXT NOT NULL DEFAULT '',
    created_by      TEXT NOT NULL DEFAULT '',
    completed_by    TEXT NOT NULL DEFAULT '',
    owner_task_id   TEXT NOT NULL DEFAULT '',
    status          INTEGER NOT NULL,
    dependent_tasks TEXT[] NOT NULL DEFAULT '{}',
    creation_date   TIMESTAMP NOT NULL,
    completion_date TIMESTAMP,
    size            BIGINT NOT NULL DEFAULT 0,
    opaque_id       BYTEA NOT NULL DEFAULT ''::BYTEA,
    manual_deletion BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_results_session_id ON results(session_id);
CREATE INDEX IF NOT EXISTS idx_results_owner_task_id ON results(owner_task_id);
CREATE INDEX IF NOT EXISTS idx_results_created_by ON results(created_by);
CREATE INDEX IF NOT EXISTS idx_results_creation_date ON results(creation_date);

-- Partitions table
CREATE TABLE IF NOT EXISTS partitions (
    partition_id         TEXT PRIMARY KEY,
    parent_partition_ids TEXT[] NOT NULL DEFAULT '{}',
    pod_reserved         INTEGER NOT NULL DEFAULT 0,
    pod_max              INTEGER NOT NULL DEFAULT 0,
    preemption_pct       INTEGER NOT NULL DEFAULT 0,
    priority             INTEGER NOT NULL DEFAULT 0,
    pod_configuration    JSONB
);

-- Auth tables
CREATE TABLE IF NOT EXISTS auth_data (
    auth_id     INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    cn          TEXT NOT NULL,
    fingerprint TEXT
);

CREATE TABLE IF NOT EXISTS user_data (
    user_id  INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    roles    INTEGER[] NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS role_data (
    role_id     INTEGER PRIMARY KEY,
    role_name   TEXT NOT NULL,
    permissions TEXT[] NOT NULL DEFAULT '{}'
);

-- LISTEN/NOTIFY triggers
CREATE OR REPLACE FUNCTION notify_task_change() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM pg_notify('task_insert', NEW.task_id || '|' || NEW.session_id);
  ELSIF TG_OP = 'UPDATE' AND OLD.status IS DISTINCT FROM NEW.status THEN
    PERFORM pg_notify('task_status', NEW.task_id || '|' || NEW.session_id || '|' || NEW.status);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_task_change ON tasks;
CREATE TRIGGER trg_task_change AFTER INSERT OR UPDATE ON tasks
FOR EACH ROW EXECUTE FUNCTION notify_task_change();

CREATE OR REPLACE FUNCTION notify_result_change() RETURNS trigger AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM pg_notify('result_insert', NEW.result_id || '|' || NEW.session_id);
  ELSIF TG_OP = 'UPDATE' THEN
    IF OLD.owner_task_id IS DISTINCT FROM NEW.owner_task_id THEN
      PERFORM pg_notify('result_owner', NEW.result_id || '|' || NEW.session_id || '|' || OLD.owner_task_id || '|' || NEW.owner_task_id);
    END IF;
    IF OLD.status IS DISTINCT FROM NEW.status THEN
      PERFORM pg_notify('result_status', NEW.result_id || '|' || NEW.session_id || '|' || NEW.status);
    END IF;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_result_change ON results;
CREATE TRIGGER trg_result_change AFTER INSERT OR UPDATE ON results
FOR EACH ROW EXECUTE FUNCTION notify_result_change();
";
}
