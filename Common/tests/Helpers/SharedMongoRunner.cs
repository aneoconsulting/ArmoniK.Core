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

using EphemeralMongo;

using MongoDB.Driver;

namespace ArmoniK.Core.Common.Tests.Helpers;

/// <summary>
///   Embedded MongoDB servers shared by all the tests of a test process.
/// </summary>
/// <remarks>
///   <para>
///     Starting a mongod for each test costs about 0.5 s on Linux and about 3 s on Windows, which made the
///     tests creating one per test the slowest of the suites. Each server is now started on first use, once
///     per process and per mode (standalone or single-node replica set), and stopped when the process exits.
///   </para>
///   <para>
///     Tests stay isolated by using their own database, created by <see cref="CreateDatabase" /> and dropped
///     when the returned <see cref="MongoTestDatabase" /> is disposed.
///   </para>
/// </remarks>
public static class SharedMongoRunner
{
  private static readonly ConcurrentDictionary<bool, Lazy<Server>> Servers = new();

  /// <summary>
  ///   Create a new database with a unique name on the shared embedded MongoDB.
  /// </summary>
  /// <param name="useSingleNodeReplicaSet">
  ///   Whether the server is a single-node replica set, needed by tests that watch collections
  /// </param>
  /// <returns>The database, dropped when disposed</returns>
  public static MongoTestDatabase CreateDatabase(bool useSingleNodeReplicaSet = false)
  {
    var server = Servers.GetOrAdd(useSingleNodeReplicaSet,
                                  rs => new Lazy<Server>(() => new Server(rs)))
                        .Value;

    return new MongoTestDatabase(server.Runner.ConnectionString,
                                 $"ArmoniK_TestDB_{Guid.NewGuid():N}",
                                 server.Client);
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
                                               Client.Dispose();
                                               Runner.Dispose();
                                             };
    }

    public IMongoRunner Runner { get; }
    public MongoClient  Client { get; }
  }
}

/// <summary>
///   A database of the shared embedded MongoDB, dedicated to one test.
/// </summary>
public sealed class MongoTestDatabase : IDisposable
{
  private readonly IMongoClient      adminClient_;
  private readonly List<MongoClient> clients_ = new();

  internal MongoTestDatabase(string       connectionString,
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
  ///   Drop the database and dispose the clients created with <see cref="CreateClient" />.
  /// </summary>
  public void Dispose()
  {
    foreach (var client in clients_)
    {
      client.Dispose();
    }

    adminClient_.DropDatabase(Name);
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
    clients_.Add(client);
    return client;
  }
}
