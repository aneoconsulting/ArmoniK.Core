// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System;
using System.Diagnostics;
using System.Threading;

using ArmoniK.Core.Adapters.TaskDB.Protocol;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.TaskDB.Tests;

[TestFixture]
public class ResultTableTests : ResultTableTestBase
{
  private TaskDbConnection? connection_;

  public override void GetResultTableInstance()
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
    ResultTable = new ResultTable(connection_,
                                   activitySource,
                                   NullLogger<ResultTable>.Instance);
  }

  [TearDown]
  public void TearDown()
  {
    connection_?.Dispose();
  }
}
