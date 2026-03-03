// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

namespace ArmoniK.Core.Adapters.TaskDB.Protocol;

/// <summary>
///   Operation codes for the TaskDB binary protocol.
///   Must stay in sync with the Go server's protocol/opcodes.go.
/// </summary>
internal static class OpCode
{
  public const byte Ping        = 0x00;
  public const byte HealthCheck = 0x01;

  // Tasks 0x10–0x2F
  public const byte AcquireTask             = 0x10;
  public const byte CreateTasks             = 0x11;
  public const byte UpdateOneTask           = 0x12;
  public const byte UpdateManyTasks         = 0x13;
  public const byte BulkUpdateTasks         = 0x14;
  public const byte RemoveRemainingDataDeps = 0x15;
  public const byte ReadTask                = 0x16;
  public const byte FindTasks               = 0x17;
  public const byte ListTasks               = 0x18;
  public const byte CountTasks              = 0x19;
  public const byte CountAllTasks           = 0x1A;
  public const byte CountPartitionTasks     = 0x1B;
  public const byte DeleteTasks             = 0x1C;
  public const byte ListApplications        = 0x1D;

  // Results 0x30–0x4F
  public const byte CreateResults         = 0x30;
  public const byte GetResults            = 0x31;
  public const byte UpdateOneResult       = 0x32;
  public const byte UpdateManyResults     = 0x33;
  public const byte BulkUpdateResults     = 0x34;
  public const byte ChangeResultOwnership = 0x35;
  public const byte SetTaskOwnership      = 0x36;
  public const byte AddTaskDependencies   = 0x37;
  public const byte AbortTaskResults      = 0x38;
  public const byte DeleteResult          = 0x39;
  public const byte DeleteResults         = 0x3A;

  // Sessions 0x50–0x5F
  public const byte CreateSession    = 0x50;
  public const byte GetSession       = 0x51;
  public const byte UpdateOneSession = 0x52;
  public const byte FindSessions     = 0x53;
  public const byte ListSessions     = 0x54;
  public const byte DeleteSession    = 0x55;

  // Partitions 0x60–0x6F
  public const byte CreatePartitions            = 0x60;
  public const byte ReadPartition               = 0x61;
  public const byte GetPartitionsWithAllocation = 0x62;
  public const byte ArePartitionsExisting       = 0x63;
  public const byte ListPartitions              = 0x64;
  public const byte FindPartitions              = 0x65;
  public const byte DeletePartition             = 0x66;
}

/// <summary>
///   Status codes returned in response frames.
/// </summary>
internal static class StatusCode
{
  public const byte Success        = 0x00;
  public const byte NotFound       = 0x01;
  public const byte AlreadyExists  = 0x02;
  public const byte FilterMismatch = 0x03;
  public const byte StreamEnd      = 0x04;
  public const byte ServerError    = 0xFF;
}
