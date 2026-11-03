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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using EphemeralMongo;

using JetBrains.Annotations;

using MongoDB.Driver;

namespace ArmoniK.Core.Common.Tests.Helpers;

/// <summary>
///   A database of an embedded MongoDB shared by all the tests of a test process, dedicated to one test.
/// </summary>
/// <remarks>
///   <para>
///     Starting a mongod for each test costs about 0.5 s on Linux and about 3 s on Windows, which made the
///     tests creating one per test the slowest of the suites. Each server is now started on first use, once
///     per process and per mode (standalone or single-node replica set), and stopped when the process exits.
///   </para>
///   <para>
///     Tests stay isolated by using their own database, created by <see cref="Create" /> and dropped when the
///     <see cref="MongoTestDatabase" /> is disposed.
///   </para>
/// </remarks>
[MustDisposeResource]
public sealed class MongoTestDatabase : IDisposable
{
  private static readonly ConcurrentDictionary<bool, Lazy<Server>> Servers = new();

  private readonly IMongoClient      adminClient_;
  private readonly List<MongoClient> clients_ = new();

  private MongoTestDatabase(string       connectionString,
                            string       name,
                            IMongoClient adminClient)
  {
    ConnectionString = connectionString;
    Name             = name;
    adminClient_     = adminClient;
  }

  /// <summary>
  ///   Connection string of the shared server
  /// </summary>
  public string ConnectionString { get; }

  /// <summary>
  ///   Name of the database dedicated to the test
  /// </summary>
  public string Name { get; }

  /// <summary>
  ///   Dispose the clients created with <see cref="CreateClient" /> and drop the database.
  /// </summary>
  /// <remarks>
  ///   Never throws: cleanup is best effort, and every step is attempted even if a previous one fails. A
  ///   database that could not be dropped only stays on the shared server until the test process exits.
  /// </remarks>
  public void Dispose()
  {
    List<MongoClient> clients;
    lock (clients_)
    {
      clients = new List<MongoClient>(clients_);
      clients_.Clear();
    }

    foreach (var client in clients)
    {
      TryRun(client.Dispose);
    }

    TryRun(() => adminClient_.DropDatabase(Name));
  }

  /// <summary>
  ///   Create a new database with a unique name on the shared embedded MongoDB.
  /// </summary>
  /// <param name="useSingleNodeReplicaSet">
  ///   Whether the server is a single-node replica set, needed by tests that watch collections
  /// </param>
  /// <returns>The database, dropped when disposed</returns>
  [MustDisposeResource]
  public static MongoTestDatabase Create(bool useSingleNodeReplicaSet = false)
  {
    var server = Servers.GetOrAdd(useSingleNodeReplicaSet,
                                  rs => new Lazy<Server>(() => new Server(rs)))
                        .Value;

    return new MongoTestDatabase(server.Runner.ConnectionString,
                                 $"ArmoniK_TestDB_{Guid.NewGuid():N}",
                                 server.Client);
  }

  /// <summary>
  ///   Create a client connected to the shared server, disposed with this database.
  /// </summary>
  /// <param name="configure">Changes applied to the client settings</param>
  /// <returns>The client</returns>
  /// <remarks>
  ///   The server outlives the test, so a client left open would keep its connections to it until the end of
  ///   the test process.
  /// </remarks>
  public MongoClient CreateClient(Action<MongoClientSettings>? configure = null)
  {
    var settings = MongoClientSettings.FromConnectionString(ConnectionString);
    configure?.Invoke(settings);
    var client = new MongoClient(settings);
    lock (clients_)
    {
      clients_.Add(client);
    }

    return client;
  }

  /// <summary>
  ///   Run a cleanup step, ignoring its failure so that the next steps still run and disposal never throws.
  /// </summary>
  /// <param name="action">The cleanup step</param>
  [SuppressMessage("Design",
                   "CA1031:Do not catch general exception types",
                   Justification = "Disposal must not throw")]
  private static void TryRun(Action action)
  {
    try
    {
      action();
    }
    catch (Exception)
    {
      // Best effort cleanup
    }
  }

  private sealed class Server
  {
    public Server(bool useSingleNodeReplicaSet)
    {
      Runner = MongoRunner.Run(MongoRunnerOptionsFactory.Create(useSingleNodeReplicaSet));
      Client = new MongoClient(Runner.ConnectionString);

      AppDomain.CurrentDomain.ProcessExit += (_,
                                              _) =>
                                             {
                                               TryRun(Client.Dispose);
                                               TryRun(Runner.Dispose);
                                             };
    }

    public IMongoRunner Runner { get; }
    public MongoClient  Client { get; }
  }
}
