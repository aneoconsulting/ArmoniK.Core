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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Injection.Options.Database;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Moq;

using Npgsql;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   Initialization of <see cref="NpgsqlConnectionProvider" />: it runs once, and it is retried while the
///   database is not reachable.
/// </summary>
[TestFixture]
public class StartupTests
{
  [SetUp]
  public void SetUp()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();
    options_      = provider.GetRequiredService<Options.PostgreSQL>();
    initDatabase_ = provider.GetRequiredService<InitDatabase>();
    logger_       = new Mock<ILogger<NpgsqlConnectionProvider>>();
  }

  [TearDown]
  public void TearDown()
    => tableProvider_?.Dispose();

  private PostgresDatabaseProvider?                tableProvider_;
  private Options.PostgreSQL?                      options_;
  private InitDatabase?                            initDatabase_;
  private Mock<ILogger<NpgsqlConnectionProvider>>? logger_;

  private const string InitializationMessage = "Initializing PostgreSQL schema";
  private const string RetryMessage          = "retrying the initialization";

  private NpgsqlConnectionProvider CreateProvider(Action<Options.PostgreSQL> configure)
  {
    var options = new Options.PostgreSQL
                  {
                    Host             = options_!.Host,
                    Port             = options_.Port,
                    User             = options_.User,
                    Password         = options_.Password,
                    DatabaseName     = options_.DatabaseName,
                    ConnectionString = options_.ConnectionString,
                  };
    configure(options);
    return new NpgsqlConnectionProvider(options,
                                        initDatabase_!,
                                        logger_!.Object);
  }

  private void VerifyLog(LogLevel level,
                         string   message,
                         Times    times)
    => logger_!.Verify(logger => logger.Log(level,
                                            It.IsAny<EventId>(),
                                            It.Is<It.IsAnyType>((state,
                                                                 _) => state.ToString()!.Contains(message)),
                                            It.IsAny<Exception?>(),
                                            It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                       times);

  private static int GetClosedPort()
  {
    var listener = new TcpListener(IPAddress.Loopback,
                                   0);
    listener.Start();
    var port = ((IPEndPoint)listener.LocalEndpoint).Port;
    listener.Stop();
    return port;
  }

  [Test]
  public async Task InitShouldRunOnceWhenCalledConcurrently()
  {
    await using var provider = CreateProvider(_ =>
                                              {
                                              });

    await Task.WhenAll(Enumerable.Range(0,
                                        5)
                                 .Select(_ => provider.Init(CancellationToken.None)))
              .ConfigureAwait(false);
    await provider.Init(CancellationToken.None)
                  .ConfigureAwait(false);

    VerifyLog(LogLevel.Information,
              InitializationMessage,
              Times.Once());
  }

  [Test]
  public async Task InitShouldRetryWhileDatabaseIsUnreachable()
  {
    await using var provider = CreateProvider(options =>
                                              {
                                                options.ConnectionString = null;
                                                options.Host             = "localhost";
                                                options.Port             = GetClosedPort();
                                                options.MaxRetries       = 2;
                                              });

    Assert.That(() => provider.Init(CancellationToken.None),
                Throws.InstanceOf<NpgsqlException>()
                      .With.Property(nameof(NpgsqlException.IsTransient))
                      .True);

    VerifyLog(LogLevel.Information,
              InitializationMessage,
              Times.Exactly(2));
    VerifyLog(LogLevel.Warning,
              RetryMessage,
              Times.Once());
  }

  [Test]
  public async Task InitShouldNotRetryWhenDatabaseDoesNotExist()
  {
    await using var provider = CreateProvider(options =>
                                              {
                                                if (options.ConnectionString is not null)
                                                {
                                                  options.ConnectionString = new NpgsqlConnectionStringBuilder(options.ConnectionString)
                                                                             {
                                                                               Database = "does_not_exist",
                                                                             }.ConnectionString;
                                                }

                                                options.DatabaseName = "does_not_exist";
                                              });

    Assert.That(() => provider.Init(CancellationToken.None),
                Throws.InstanceOf<PostgresException>()
                      .With.Property(nameof(PostgresException.SqlState))
                      .EqualTo(PostgresErrorCodes.InvalidCatalogName));

    VerifyLog(LogLevel.Information,
              InitializationMessage,
              Times.Once());
    VerifyLog(LogLevel.Warning,
              RetryMessage,
              Times.Never());
  }
}
