// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.

using System;
using System.Collections.Generic;
using System.Linq;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Storage;

using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Adapters.TaskDB.Protocol;

/// <summary>
///   Bidirectional mapping between ArmoniK domain types and TaskDB wire types.
/// </summary>
internal static class Mapping
{
  // ── TaskStatus ───────────────────────────────────────────────────────────

  public static WireTaskStatus ToWire(this TaskStatus s)
    => s switch
    {
      TaskStatus.Unspecified => WireTaskStatus.Unspecified,
      TaskStatus.Creating    => WireTaskStatus.Creating,
      TaskStatus.Submitted   => WireTaskStatus.Submitted,
      TaskStatus.Dispatched  => WireTaskStatus.Dispatched,
      TaskStatus.Completed   => WireTaskStatus.Completed,
      TaskStatus.Error       => WireTaskStatus.Error,
      TaskStatus.Timeout     => WireTaskStatus.Timeout,
      TaskStatus.Cancelling  => WireTaskStatus.Cancelling,
      TaskStatus.Cancelled   => WireTaskStatus.Cancelled,
      TaskStatus.Processing  => WireTaskStatus.Processing,
      TaskStatus.Processed   => WireTaskStatus.Processed,
      TaskStatus.Retried     => WireTaskStatus.Retried,
      TaskStatus.Pending     => WireTaskStatus.Pending,
      TaskStatus.Paused      => WireTaskStatus.Paused,
      _                      => WireTaskStatus.Unspecified,
    };

  public static TaskStatus ToDomain(this WireTaskStatus s)
    => s switch
    {
      WireTaskStatus.Unspecified => TaskStatus.Unspecified,
      WireTaskStatus.Creating    => TaskStatus.Creating,
      WireTaskStatus.Submitted   => TaskStatus.Submitted,
      WireTaskStatus.Dispatched  => TaskStatus.Dispatched,
      WireTaskStatus.Completed   => TaskStatus.Completed,
      WireTaskStatus.Error       => TaskStatus.Error,
      WireTaskStatus.Timeout     => TaskStatus.Timeout,
      WireTaskStatus.Cancelling  => TaskStatus.Cancelling,
      WireTaskStatus.Cancelled   => TaskStatus.Cancelled,
      WireTaskStatus.Processing  => TaskStatus.Processing,
      WireTaskStatus.Processed   => TaskStatus.Processed,
      WireTaskStatus.Retried     => TaskStatus.Retried,
      WireTaskStatus.Pending     => TaskStatus.Pending,
      WireTaskStatus.Paused      => TaskStatus.Paused,
      _                          => TaskStatus.Unspecified,
    };

  // ── ResultStatus ─────────────────────────────────────────────────────────

  public static WireResultStatus ToWire(this ResultStatus s)
    => s switch
    {
      ResultStatus.Unspecified   => WireResultStatus.Unspecified,
      ResultStatus.Created       => WireResultStatus.Created,
      ResultStatus.Completed     => WireResultStatus.Completed,
      ResultStatus.Aborted       => WireResultStatus.Aborted,
      ResultStatus.DeletedData   => WireResultStatus.DeletedData,
      _                          => WireResultStatus.Unspecified,
    };

  public static ResultStatus ToDomain(this WireResultStatus s)
    => s switch
    {
      WireResultStatus.Unspecified => ResultStatus.Unspecified,
      WireResultStatus.Created     => ResultStatus.Created,
      WireResultStatus.Completed   => ResultStatus.Completed,
      WireResultStatus.Aborted     => ResultStatus.Aborted,
      WireResultStatus.DeletedData => ResultStatus.DeletedData,
      _                            => ResultStatus.Unspecified,
    };

  // ── SessionStatus ────────────────────────────────────────────────────────

  public static WireSessionStatus ToWire(this SessionStatus s)
    => s switch
    {
      SessionStatus.Unspecified => WireSessionStatus.Unspecified,
      SessionStatus.Running     => WireSessionStatus.Running,
      SessionStatus.Cancelled   => WireSessionStatus.Cancelled,
      SessionStatus.Paused      => WireSessionStatus.Paused,
      SessionStatus.Closed      => WireSessionStatus.Closed,
      SessionStatus.Purged      => WireSessionStatus.Purged,
      SessionStatus.Deleted     => WireSessionStatus.Deleted,
      _                         => WireSessionStatus.Unspecified,
    };

  public static SessionStatus ToDomain(this WireSessionStatus s)
    => s switch
    {
      WireSessionStatus.Unspecified => SessionStatus.Unspecified,
      WireSessionStatus.Running     => SessionStatus.Running,
      WireSessionStatus.Cancelled   => SessionStatus.Cancelled,
      WireSessionStatus.Paused      => SessionStatus.Paused,
      WireSessionStatus.Closed      => SessionStatus.Closed,
      WireSessionStatus.Purged      => SessionStatus.Purged,
      WireSessionStatus.Deleted     => SessionStatus.Deleted,
      _                             => SessionStatus.Unspecified,
    };

  // ── Output ───────────────────────────────────────────────────────────────

  public static WireOutput ToWire(this Output o)
    => new()
    {
      Status = o.Status switch
      {
        OutputStatus.Success => WireOutputStatus.Success,
        OutputStatus.Error   => WireOutputStatus.Error,
        OutputStatus.Timeout => WireOutputStatus.Timeout,
        _                    => WireOutputStatus.Error,
      },
      Error = o.Error ?? "",
    };

  public static Output ToDomain(this WireOutput? w)
    => w is null
       ? new Output(OutputStatus.Error, "")
       : new Output(w.Status switch
                    {
                      WireOutputStatus.Success => OutputStatus.Success,
                      WireOutputStatus.Error   => OutputStatus.Error,
                      WireOutputStatus.Timeout => OutputStatus.Timeout,
                      _                        => OutputStatus.Error,
                    },
                    w.Error);

  // ── TaskOptions ──────────────────────────────────────────────────────────

  public static WireTaskOptions ToWire(this TaskOptions o)
    => new()
    {
      Options              = o.Options?.ToDictionary(kv => kv.Key, kv => kv.Value) ?? new(),
      MaxDurationTicks     = o.MaxDuration.Ticks,
      MaxRetries           = o.MaxRetries,
      Priority             = o.Priority,
      PartitionId          = o.PartitionId,
      ApplicationName      = o.ApplicationName,
      ApplicationVersion   = o.ApplicationVersion,
      ApplicationNamespace = o.ApplicationNamespace,
      ApplicationService   = o.ApplicationService,
      EngineType           = o.EngineType,
    };

  public static TaskOptions ToDomain(this WireTaskOptions w)
    => new(w.Options,
           TimeSpan.FromTicks(w.MaxDurationTicks),
           w.MaxRetries,
           w.Priority,
           w.PartitionId,
           w.ApplicationName,
           w.ApplicationVersion,
           w.ApplicationNamespace,
           w.ApplicationService,
           w.EngineType);

  // ── TaskData ─────────────────────────────────────────────────────────────

  public static WireTaskData ToWire(this TaskData t)
    => new()
    {
      TaskId                    = t.TaskId,
      ParentTaskIds             = t.ParentTaskIds.ToList(),
      DataDependencies          = t.DataDependencies.ToList(),
      RemainingDataDependencies = t.RemainingDataDependencies.Keys.ToList(),
      ExpectedOutputIds         = t.ExpectedOutputIds.ToList(),
      RetryOfIds                = t.RetryOfIds.ToList(),
      Status                    = t.Status.ToWire(),
      SessionId                 = t.SessionId,
      InitialTaskId             = t.InitialTaskId,
      OwnerPodId                = t.OwnerPodId,
      OwnerPodName              = t.OwnerPodName,
      CreatedBy                 = t.CreatedBy,
      Options                   = t.Options.ToWire(),
      Output                    = t.Output?.ToWire(),
      CreationDate              = t.CreationDate,
      SubmittedDate             = t.SubmittedDate ?? DateTime.MinValue,
      StartDate                 = t.StartDate ?? DateTime.MinValue,
      EndDate                   = t.EndDate ?? DateTime.MinValue,
      ReceptionDate             = t.ReceptionDate ?? DateTime.MinValue,
      AcquisitionDate           = t.AcquisitionDate ?? DateTime.MinValue,
      ProcessedDate             = t.ProcessedDate ?? DateTime.MinValue,
      FetchedDate               = t.FetchedDate ?? DateTime.MinValue,
      PodTtl                    = t.PodTtl ?? DateTime.MinValue,
      PayloadId                 = t.PayloadId,
    };

  public static TaskData ToDomain(this WireTaskData w)
  => new(w.SessionId,
         w.TaskId,
         w.OwnerPodId,
         w.OwnerPodName,
         w.PayloadId,
         w.ParentTaskIds,
         w.DataDependencies,
         w.RemainingDataDependencies.ToDictionary(id => id, _ => true),
         w.ExpectedOutputIds,
         w.InitialTaskId,
         w.CreatedBy,
         w.RetryOfIds,
         w.Status.ToDomain(),
         "",                              // StatusMessage — not in wire format
         w.Options.ToDomain(),
         w.CreationDate,                  // non-nullable DateTime, no NullIfMinValue
         NullIfMinValue(w.SubmittedDate),
         NullIfMinValue(w.StartDate),
         NullIfMinValue(w.EndDate),
         NullIfMinValue(w.ReceptionDate),
         NullIfMinValue(w.AcquisitionDate),
         NullIfMinValue(w.ProcessedDate),
         NullIfMinValue(w.FetchedDate),
         NullIfMinValue(w.PodTtl),
         null,                            // ProcessingToEndDuration — not in wire format
         null,                            // CreationToEndDuration   — not in wire format
         null,                            // ReceivedToEndDuration   — not in wire format
         w.Output?.ToDomain());

  // ── Result ───────────────────────────────────────────────────────────────

  public static WireResultData ToWire(this Result r)
    => new()
    {
      ResultId       = r.ResultId,
      Name           = r.Name,
      SessionId      = r.SessionId,
      OwnerTaskId    = r.OwnerTaskId,
      CreatedBy      = r.CreatedBy,
      CompletedBy    = r.CompletedBy,
      Status         = r.Status.ToWire(),
      DependentTasks = r.DependentTasks.ToList(),
      CreationDate   = r.CreationDate,
      CompletionDate = r.CompletionDate ?? DateTime.MinValue,
      Size           = r.Size,
    };

    public static Result ToDomain(this WireResultData w)
    => new(w.SessionId, w.ResultId, w.Name, w.CreatedBy, w.CompletedBy, w.OwnerTaskId,
            w.Status.ToDomain(), w.DependentTasks,
            w.CreationDate, NullIfMinValue(w.CompletionDate),
            w.Size,
            Array.Empty<byte>(),  // OpaqueId — not in wire format yet
            false);    

  // ── SessionData ──────────────────────────────────────────────────────────

  public static WireSessionData ToWire(this SessionData s)
    => new()
    {
      SessionId        = s.SessionId,
      Status           = s.Status.ToWire(),
      PartitionIds     = s.PartitionIds.ToList(),
      Options          = s.Options.ToWire(),
      CreationDate     = s.CreationDate,
      CancellationDate = s.CancellationDate ?? DateTime.MinValue,
      ClosureDate      = s.ClosureDate ?? DateTime.MinValue,
      PurgeDate        = s.PurgeDate ?? DateTime.MinValue,
      DeletionTtl      = s.DeletionTtl ?? DateTime.MinValue,
      DurationTicks    = s.Duration?.Ticks ?? 0,
    };

public static SessionData ToDomain(this WireSessionData w)
  => new(w.SessionId,
         w.Status.ToDomain(),
         true,  // ClientSubmission
         true,  // WorkerSubmission
         w.CreationDate,
         NullIfMinValue(w.CancellationDate),
         NullIfMinValue(w.ClosureDate),
         NullIfMinValue(w.PurgeDate),
         NullIfMinValue(w.DeleteDate),   // was: DeleteDate
         NullIfMinValue(w.DeletionTtl),
         w.DurationTicks == 0 ? null : TimeSpan.FromTicks(w.DurationTicks),
         w.PartitionIds,   // ← was missing
         w.Options.ToDomain());
  // ── PartitionData ────────────────────────────────────────────────────────

  public static WirePartitionData ToWire(this PartitionData p)
    => new()
    {
      PartitionId          = p.PartitionId,
      ParentPartitionIds   = p.ParentPartitionIds.ToList(),
      PodReserved          = p.PodReserved,
      PodMax               = p.PodMax,
      PreemptionPercentage = p.PreemptionPercentage,
      Priority             = p.Priority,
      PodConfiguration = p.PodConfiguration?.Configuration.ToDictionary(kv => kv.Key, kv => kv.Value) ?? new(),
    };

    public static PartitionData ToDomain(this WirePartitionData w)
    => new(w.PartitionId,
            w.ParentPartitionIds,
            w.PodReserved,
            w.PodMax,
            w.PreemptionPercentage,
            w.Priority,
            w.PodConfiguration.Count == 0
                ? null
                : new PodConfiguration(w.PodConfiguration));

  // ── UpdateDefinition → FieldUpdates ──────────────────────────────────────

  /// <summary>
  ///   Converts an ArmoniK UpdateDefinition into a list of wire FieldUpdates.
  ///   Uses the property expression to extract the field name as a string.
  /// </summary>
  public static List<WireFieldUpdate> ToWireUpdates<T>(this Core.Common.Storage.UpdateDefinition<T> updates)
    => updates.Setters
              .Select(s =>
              {
                var name = ExtractMemberName(s.Property);
                return new WireFieldUpdate
                {
                  FieldName = name,
                  Value     = s.Value,
                };
              })
              .ToList();

  private static string ExtractMemberName(System.Linq.Expressions.LambdaExpression expr)
  {
    // Unwrap casts: (object)x.SomeField → SomeField
    var body = expr.Body;
    if (body is System.Linq.Expressions.UnaryExpression unary)
      body = unary.Operand;

    if (body is System.Linq.Expressions.MemberExpression member)
      return member.Member.Name;

    // Fallback: use the expression string (expensive but safe)
    return body.ToString();
  }

  // ── Helpers ──────────────────────────────────────────────────────────────

  private static DateTime? NullIfMinValue(DateTime dt)
    => dt == DateTime.MinValue ? null : dt;
}
