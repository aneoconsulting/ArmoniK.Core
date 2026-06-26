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
using System.IO;
using System.Threading;

using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using MysticMind.PostgresEmbed;

using Npgsql;

using Serilog;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

internal class PostgresDatabaseProvider : IDisposable
{
  private const           string         DatabaseName   = "armonik_test";
  private const           string         PgUser         = "postgres";
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Adapters.PostgresSQL.Tests");

  private static readonly object   Lock = new();
  private static          PgServer? sharedServer_;
  private static          int      sharedPort_;
  private static          string?  sharedConnectionString_;

  static PostgresDatabaseProvider()
  {
#pragma warning disable CS0618 // Type or member is obsolete
    AppContext.SetSwitch("Npgsql.EnableLegacyTimestampBehavior",
                         true);
#pragma warning restore CS0618
  }

  private readonly ServiceProvider provider_;

  public PostgresDatabaseProvider()
  {
    EnsureServerStarted();
    TruncateAllTables();

    var loggerSerilog = new LoggerConfiguration().WriteTo.Console()
                                                 .Enrich.FromLogContext()
                                                 .CreateLogger();

    var logger = LoggerFactory.Create(builder => builder.AddSerilog(loggerSerilog))
                              .CreateLogger("root");

    Dictionary<string, string?> minimalConfig = new()
                                                {
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.TableStorage)}",
                                                    "ArmoniK.Adapters.PostgresSQL.TableStorage"
                                                  },
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.AuthenticationStorage)}",
                                                    "ArmoniK.Adapters.PostgresSQL.AuthenticationTable"
                                                  },
                                                  {
                                                    $"{Options.TableStorage.SettingSection}:{nameof(Options.TableStorage.PollingDelayMax)}",
                                                    "00:00:10"
                                                  },
                                                };

    // When using an external PostgreSQL (POSTGRES_TEST_CONNECTION_STRING set), sharedPort_ is 0.
    // Use the ConnectionString option directly; otherwise configure individual fields from pg_embed.
    if (sharedPort_ == 0)
    {
      minimalConfig[$"{Options.PostgreSQL.SettingSection}:{nameof(Options.PostgreSQL.ConnectionString)}"] = sharedConnectionString_;
    }
    else
    {
      minimalConfig[$"{Options.PostgreSQL.SettingSection}:{nameof(Options.PostgreSQL.Host)}"]         = "localhost";
      minimalConfig[$"{Options.PostgreSQL.SettingSection}:{nameof(Options.PostgreSQL.Port)}"]         = sharedPort_.ToString();
      minimalConfig[$"{Options.PostgreSQL.SettingSection}:{nameof(Options.PostgreSQL.User)}"]         = PgUser;
      minimalConfig[$"{Options.PostgreSQL.SettingSection}:{nameof(Options.PostgreSQL.DatabaseName)}"] = DatabaseName;
    }

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig)
                 .AddEnvironmentVariables();

    var serviceCollection = new ServiceCollection();
    serviceCollection.AddPostgresComponents(configuration,
                                            logger);
    serviceCollection.AddClientSubmitterAuthenticationStorage(configuration);
    serviceCollection.AddSingleton(ActivitySource);
    serviceCollection.AddInitializedOption<InitServices>(configuration,
                                                         InitServices.SettingSection);
    serviceCollection.AddSingleton<InitDatabase>();

    serviceCollection.AddLogging(builder => builder.AddSerilog(loggerSerilog));

    provider_ = serviceCollection.BuildServiceProvider(new ServiceProviderOptions
                                                       {
                                                         ValidateOnBuild = true,
                                                       });
  }

  public void Dispose()
    => provider_.Dispose();

  public IServiceProvider GetServiceProvider()
    => provider_;

  private static void EnsureServerStarted()
  {
    if (sharedConnectionString_ is not null)
    {
      return;
    }

    lock (Lock)
    {
      if (sharedConnectionString_ is not null)
      {
        return;
      }

      var externalConnStr = Environment.GetEnvironmentVariable("POSTGRES_TEST_CONNECTION_STRING");
      if (externalConnStr is not null)
      {
        // Use an externally managed PostgreSQL (e.g. `docker run -d -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17`).
        // The database must already exist.
        sharedConnectionString_ = externalConnStr;
        return;
      }

      // Persist binaries and data outside the build output so they survive
      // `dotnet clean` and are reused across runs (much faster startup).
      var pgDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                               "armonik-pg-embed");

      var server = new PgServer("17.4.0",
                                pgUser: PgUser,
                                dbDir: pgDir,
                                addLocalUserAccessPermission: true,
                                // Keep the data directory between runs; TruncateAllTables handles cleanup.
                                clearWorkingDirOnStart: false,
                                locale: "C");
      server.Start();

      var port             = server.PgPort;
      var connectionString = $"Host=localhost;Port={port};Database={DatabaseName};Username={PgUser};Pooling=false";

      WaitForServerAndCreateDatabase(port);

      sharedServer_           = server;
      sharedPort_             = port;
      sharedConnectionString_ = connectionString;

      AppDomain.CurrentDomain.ProcessExit += (_,
                                              _) =>
                                             {
                                               sharedServer_?.Dispose();
                                               sharedServer_ = null;
                                             };
    }
  }

  private static void WaitForServerAndCreateDatabase(int port)
  {
    var connectionString = $"Host=localhost;Port={port};Database=postgres;Username={PgUser};Timeout=5;Pooling=false";
    for (var i = 0; i < 60; i++)
    {
      try
      {
        using var conn = new NpgsqlConnection(connectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"CREATE DATABASE \"{DatabaseName}\"";
        cmd.ExecuteNonQuery();
        return;
      }
      catch (Npgsql.PostgresException ex) when (ex.SqlState == "42P04") // duplicate_database
      {
        // Database already exists (clearWorkingDirOnStart=false reuses existing data dir).
        return;
      }
      catch (NpgsqlException)
      {
        Thread.Sleep(500);
      }
    }

    throw new TimeoutException("Embedded PostgreSQL server did not become ready in time");
  }

  private static void TruncateAllTables()
  {
    using var conn = new NpgsqlConnection(sharedConnectionString_);
    conn.Open();
    using var cmd = conn.CreateCommand();
    cmd.CommandTimeout = 30;
    cmd.CommandText    = "DO $$ DECLARE r RECORD; BEGIN " +
                         "FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP " +
                         "EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' CASCADE'; " +
                         "END LOOP; END $$;";
    cmd.ExecuteNonQuery();
  }
}
