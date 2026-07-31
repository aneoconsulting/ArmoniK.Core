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

using EphemeralMongo;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

/// <summary>
///   Builds the <see cref="MongoRunnerOptions" /> every test provider runs its embedded MongoDB with.
/// </summary>
/// <remarks>
///   These options used to be copied into each provider, which is how the replica-set timeout below
///   ended up set in only one of them. Everything that should hold for every embedded MongoDB in the
///   test suites belongs here.
/// </remarks>
public static class MongoRunnerOptionsFactory
{
  /// <summary>
  ///   Create the options for an embedded MongoDB.
  /// </summary>
  /// <param name="useSingleNodeReplicaSet">
  ///   Whether the instance is a single-node replica set, needed by tests that watch collections
  /// </param>
  /// <param name="logger">Logger the mongod output is forwarded to, or null to discard it</param>
  /// <returns>Options to hand to <see cref="MongoRunner.Run" /></returns>
  public static MongoRunnerOptions Create(bool     useSingleNodeReplicaSet = false,
                                          ILogger? logger                  = null)
  {
    var options = new MongoRunnerOptions
                  {
                    UseSingleNodeReplicaSet = useSingleNodeReplicaSet,
#pragma warning disable CA2254 // log inputs should be constant
                    StandardOutputLogger = logger is null
                                             ? null
                                             : line => logger.LogInformation(line),
                    StandardErrorLogger = logger is null
                                            ? null
                                            : line => logger.LogError(line),
#pragma warning restore CA2254
                    // Starting a single-node replica set means launching mongod, running rs.initiate
                    // and waiting for the node to reach PRIMARY, which does not fit in EphemeralMongo's
                    // 10s default on a loaded CI runner - the Windows runner installs no mongod, so it
                    // also has to download and unpack one first. The same timeout additionally guards
                    // EphemeralMongo's wait for the cluster to accept transactions.
                    ReplicaSetSetupTimeout = TimeSpan.FromSeconds(30),
                  };

    // Lets CI point at a preinstalled mongod instead of downloading one.
    var binDir = Environment.GetEnvironmentVariable("EphemeralMongo__BinaryDirectory");
    if (!string.IsNullOrEmpty(binDir))
    {
      options.BinaryDirectory = binDir;
    }

    return options;
  }
}
