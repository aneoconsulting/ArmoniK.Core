// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System;
using System.Diagnostics;
using System.Threading;

using ArmoniK.Core.Adapters.TaskDB.Protocol;
using ArmoniK.Core.Common.Tests.Storage;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.TaskDB.Tests;

[TestFixture]
public class SessionTableTests : SessionTableTestBase
{
  private TaskDbConnection? connection_;

  public override void GetSessionTableInstance()
  {
    var addr    = Environment.GetEnvironmentVariable("TASKDB_ADDRESS") ?? "localhost:7890";
    var parts   = addr.Split(':');
    var options = new Options.TaskDB
    {
      Host = parts[0],
      Port = parts.Length > 1 ? int.Parse(parts[1]) : 7890,
    };

    connection_ = new TaskDbConnection(options,
                                        NullLogger<TaskDbConnection>.Instance);
    connection_.ConnectAsync(CancellationToken.None).GetAwaiter().GetResult();

    var activitySource = new ActivitySource("Tests");
    SessionTable = new SessionTable(connection_,
                                     activitySource,
                                     NullLogger<SessionTable>.Instance);
  }

  [TearDown]
  public void TearDown()
  {
    connection_?.Dispose();
  }
}

[TestFixture]
public class PartitionTableTests : PartitionTableTestBase
{
  private TaskDbConnection? connection_;

  public override void GetPartitionTableInstance()
  {
    var addr    = Environment.GetEnvironmentVariable("TASKDB_ADDRESS") ?? "localhost:7890";
    var parts   = addr.Split(':');
    var options = new Options.TaskDB
    {
      Host = parts[0],
      Port = parts.Length > 1 ? int.Parse(parts[1]) : 7890,
    };

    connection_ = new TaskDbConnection(options,
                                        NullLogger<TaskDbConnection>.Instance);
    connection_.ConnectAsync(CancellationToken.None).GetAwaiter().GetResult();

    var activitySource = new ActivitySource("Tests");
    PartitionTable = new PartitionTable(connection_,
                                         activitySource,
                                         NullLogger<PartitionTable>.Instance);
  }

  [TearDown]
  public void TearDown()
  {
    connection_?.Dispose();
  }
}
