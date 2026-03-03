// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
//
// Wire-protocol message DTOs.
// Must stay in sync with the Go server's messages/types.go.

using System;
using System.Collections.Generic;

using MessagePack;

namespace ArmoniK.Core.Adapters.TaskDB.Protocol;

// ── Enums ────────────────────────────────────────────────────────────────────

internal enum WireTaskStatus : int
{
  Unspecified = 0,
  Creating    = 1,
  Submitted   = 2,
  Dispatched  = 3,
  Completed   = 4,
  Error       = 5,
  Timeout     = 6,
  Cancelling  = 7,
  Cancelled   = 8,
  Processing  = 9,
  Processed   = 10,
  Retried     = 11,
  Pending     = 12,
  Paused      = 13,
}

internal enum WireSessionStatus : int
{
  Unspecified = 0,
  Running     = 1,
  Cancelled   = 2,
  Paused      = 3,
  Closed      = 4,
  Purged      = 5,
  Deleted     = 6,
}

internal enum WireResultStatus : int
{
  Unspecified   = 0,
  Created       = 1,
  Completed     = 2,
  Aborted       = 3,
  DeletedData   = 4,
}

internal enum WireOutputStatus : int
{
  Success = 0,
  Error   = 1,
  Timeout = 2,
}

// ── Shared building blocks ───────────────────────────────────────────────────

[MessagePackObject]
internal class WireOutput
{
  [Key("Status")] public WireOutputStatus Status { get; set; }
  [Key("Error")]  public string           Error  { get; set; } = "";
}

[MessagePackObject]
internal class WireTaskOptions
{
  [Key("Options")]              public Dictionary<string, string> Options              { get; set; } = new();
  [Key("MaxDuration")]          public long                       MaxDurationTicks     { get; set; }
  [Key("MaxRetries")]           public int                        MaxRetries           { get; set; }
  [Key("Priority")]             public int                        Priority             { get; set; }
  [Key("PartitionId")]          public string                     PartitionId          { get; set; } = "";
  [Key("ApplicationName")]      public string                     ApplicationName      { get; set; } = "";
  [Key("ApplicationVersion")]   public string                     ApplicationVersion   { get; set; } = "";
  [Key("ApplicationNamespace")] public string                     ApplicationNamespace { get; set; } = "";
  [Key("ApplicationService")]   public string                     ApplicationService   { get; set; } = "";
  [Key("EngineType")]           public string                     EngineType           { get; set; } = "";
}

[MessagePackObject]
internal class WireTaskData
{
  [Key("TaskId")]                    public string             TaskId                    { get; set; } = "";
  [Key("ParentTaskIds")]             public List<string>       ParentTaskIds             { get; set; } = new();
  [Key("DataDependencies")]          public List<string>       DataDependencies          { get; set; } = new();
  [Key("RemainingDataDependencies")] public List<string>       RemainingDataDependencies { get; set; } = new();
  [Key("ExpectedOutputIds")]         public List<string>       ExpectedOutputIds         { get; set; } = new();
  [Key("RetryOfIds")]                public List<string>       RetryOfIds                { get; set; } = new();
  [Key("Status")]                    public WireTaskStatus     Status                    { get; set; }
  [Key("SessionId")]                 public string             SessionId                 { get; set; } = "";
  [Key("InitialTaskId")]             public string             InitialTaskId             { get; set; } = "";
  [Key("OwnerPodId")]                public string             OwnerPodId                { get; set; } = "";
  [Key("OwnerPodName")]              public string             OwnerPodName              { get; set; } = "";
  [Key("CreatedBy")]                 public string             CreatedBy                 { get; set; } = "";
  [Key("Options")]                   public WireTaskOptions    Options                   { get; set; } = new();
  [Key("Output")]                    public WireOutput?        Output                    { get; set; }
  [Key("CreationDate")]              public DateTime           CreationDate              { get; set; }
  [Key("SubmittedDate")]             public DateTime           SubmittedDate             { get; set; }
  [Key("StartDate")]                 public DateTime           StartDate                 { get; set; }
  [Key("EndDate")]                   public DateTime           EndDate                   { get; set; }
  [Key("ReceptionDate")]             public DateTime           ReceptionDate             { get; set; }
  [Key("AcquisitionDate")]           public DateTime           AcquisitionDate           { get; set; }
  [Key("ProcessedDate")]             public DateTime           ProcessedDate             { get; set; }
  [Key("FetchedDate")]               public DateTime           FetchedDate               { get; set; }
  [Key("PodTtl")]                    public DateTime           PodTtl                    { get; set; }
  [Key("PayloadId")]                 public string             PayloadId                 { get; set; } = "";
}

[MessagePackObject]
internal class WireResultData
{
  [Key("ResultId")]       public string           ResultId       { get; set; } = "";
  [Key("Name")]           public string           Name           { get; set; } = "";
  [Key("SessionId")]      public string           SessionId      { get; set; } = "";
  [Key("OwnerTaskId")]    public string           OwnerTaskId    { get; set; } = "";
  [Key("CreatedBy")]      public string           CreatedBy      { get; set; } = "";
  [Key("CompletedBy")]    public string           CompletedBy    { get; set; } = "";
  [Key("Status")]         public WireResultStatus Status         { get; set; }
  [Key("DependentTasks")] public List<string>     DependentTasks { get; set; } = new();
  [Key("CreationDate")]   public DateTime         CreationDate   { get; set; }
  [Key("CompletionDate")] public DateTime         CompletionDate { get; set; }
  [Key("Size")]           public long             Size           { get; set; }
}

[MessagePackObject]
internal class WireSessionData
{
  [Key("SessionId")]        public string           SessionId        { get; set; } = "";
  [Key("Status")]           public WireSessionStatus Status          { get; set; }
  [Key("PartitionIds")]     public List<string>     PartitionIds     { get; set; } = new();
  [Key("Options")]          public WireTaskOptions  Options          { get; set; } = new();
  [Key("CreationDate")]     public DateTime         CreationDate     { get; set; }
  [Key("CancellationDate")] public DateTime         CancellationDate { get; set; }
  [Key("ClosureDate")]      public DateTime         ClosureDate      { get; set; }
  [Key("PurgeDate")]        public DateTime         PurgeDate        { get; set; }
  [Key("DeleteDate")]       public DateTime         DeleteDate       { get; set; }
  [Key("DeletionTtl")]      public DateTime         DeletionTtl      { get; set; }
  [Key("Duration")]         public long             DurationTicks    { get; set; }
}

[MessagePackObject]
internal class WirePartitionData
{
  [Key("PartitionId")]          public string              PartitionId          { get; set; } = "";
  [Key("ParentPartitionIds")]   public List<string>        ParentPartitionIds   { get; set; } = new();
  [Key("PodReserved")]          public int                 PodReserved          { get; set; }
  [Key("PodMax")]               public int                 PodMax               { get; set; }
  [Key("PreemptionPercentage")] public int                 PreemptionPercentage { get; set; }
  [Key("Priority")]             public int                 Priority             { get; set; }
  [Key("PodConfiguration")]     public Dictionary<string, string> PodConfiguration { get; set; } = new();
}

// ── Coarse filter ────────────────────────────────────────────────────────────

[MessagePackObject]
internal class WireCoarseFilter
{
  [Key("SessionId")]   public string?             SessionId   { get; set; }
  [Key("StatusIn")]    public List<WireTaskStatus>? StatusIn  { get; set; }
  [Key("PartitionId")] public string?             PartitionId { get; set; }
  [Key("TaskIds")]     public List<string>?       TaskIds     { get; set; }
}

[MessagePackObject]
internal class WireCoarseResultFilter
{
  [Key("SessionId")]   public string?                SessionId   { get; set; }
  [Key("StatusIn")]    public List<WireResultStatus>? StatusIn   { get; set; }
  [Key("TaskIds")]     public List<string>?           TaskIds     { get; set; }
  [Key("ResultIds")]   public List<string>?           ResultIds   { get; set; }
}

[MessagePackObject]
internal class WireCoarseSessionFilter
{
  [Key("SessionIds")] public List<string>?              SessionIds { get; set; }
  [Key("StatusIn")]   public List<WireSessionStatus>?   StatusIn   { get; set; }
}

[MessagePackObject]
internal class WireCoarsePartitionFilter
{
  [Key("PartitionIds")] public List<string>? PartitionIds { get; set; }
}

// ── Field update ─────────────────────────────────────────────────────────────

[MessagePackObject]
internal class WireFieldUpdate
{
  [Key("FieldName")] public string  FieldName { get; set; } = "";
  [Key("Value")]     public object? Value     { get; set; }
}

// ── Task request messages ────────────────────────────────────────────────────

[MessagePackObject]
internal class WireAcquireTaskRequest
{
  [Key("TaskId")]          public string   TaskId          { get; set; } = "";
  [Key("OwnerPodId")]      public string   OwnerPodId      { get; set; } = "";
  [Key("OwnerPodName")]    public string   OwnerPodName    { get; set; } = "";
  [Key("ReceptionDate")]   public DateTime ReceptionDate   { get; set; }
  [Key("AcquisitionDate")] public DateTime AcquisitionDate { get; set; }
}

[MessagePackObject]
internal class WireCreateTasksRequest
{
  [Key("Tasks")] public List<WireTaskData> Tasks { get; set; } = new();
}

[MessagePackObject]
internal class WireUpdateOneTaskRequest
{
  [Key("TaskId")]  public string              TaskId  { get; set; } = "";
  [Key("Filter")]  public WireCoarseFilter?   Filter  { get; set; }
  [Key("Updates")] public List<WireFieldUpdate> Updates { get; set; } = new();
  [Key("Before")]  public bool                Before  { get; set; }
}

[MessagePackObject]
internal class WireUpdateManyTasksRequest
{
  [Key("Filter")]  public WireCoarseFilter       Filter  { get; set; } = new();
  [Key("Updates")] public List<WireFieldUpdate>  Updates { get; set; } = new();
}

[MessagePackObject]
internal class WireBulkUpdateItem
{
  [Key("Filter")]  public WireCoarseFilter       Filter  { get; set; } = new();
  [Key("Updates")] public List<WireFieldUpdate>  Updates { get; set; } = new();
}

[MessagePackObject]
internal class WireBulkUpdateTasksRequest
{
  [Key("Items")] public List<WireBulkUpdateItem> Items { get; set; } = new();
}

[MessagePackObject]
internal class WireRemoveDepRequest
{
  [Key("TaskIds")]       public List<string> TaskIds       { get; set; } = new();
  [Key("DependencyIds")] public List<string> DependencyIds { get; set; } = new();
}

[MessagePackObject]
internal class WireRemoveDepResponse
{
  [Key("ReadyTasks")] public List<WireTaskData> ReadyTasks { get; set; } = new();
}

[MessagePackObject]
internal class WireReadTaskRequest
{
  [Key("TaskId")] public string TaskId { get; set; } = "";
}

[MessagePackObject]
internal class WireFindTasksRequest
{
  [Key("Filter")] public WireCoarseFilter Filter { get; set; } = new();
}

[MessagePackObject]
internal class WireListTasksRequest
{
  [Key("Filter")]    public WireCoarseFilter Filter    { get; set; } = new();
  [Key("OrderBy")]   public string           OrderBy   { get; set; } = "";
  [Key("Ascending")] public bool             Ascending { get; set; }
  [Key("Page")]      public int              Page      { get; set; }
  [Key("PageSize")]  public int              PageSize  { get; set; }
}

[MessagePackObject]
internal class WireListTasksResponse
{
  [Key("Tasks")]      public List<WireTaskData> Tasks      { get; set; } = new();
  [Key("TotalCount")] public long               TotalCount { get; set; }
}

[MessagePackObject]
internal class WireCountTasksRequest
{
  [Key("Filter")] public WireCoarseFilter Filter { get; set; } = new();
}

[MessagePackObject]
internal class WireCountTasksResponse
{
  [Key("Counts")] public Dictionary<int, long> Counts { get; set; } = new();
}

[MessagePackObject]
internal class WireDeleteTasksRequest
{
  [Key("TaskIds")]   public List<string>? TaskIds   { get; set; }
  [Key("SessionId")] public string?       SessionId { get; set; }
}

[MessagePackObject]
internal class WireListApplicationsRequest
{
  [Key("Filter")]    public WireCoarseFilter Filter    { get; set; } = new();
  [Key("OrderBy")]   public string           OrderBy   { get; set; } = "";
  [Key("Ascending")] public bool             Ascending { get; set; }
  [Key("Page")]      public int              Page      { get; set; }
  [Key("PageSize")]  public int              PageSize  { get; set; }
}

[MessagePackObject]
internal class WireApplicationEntry
{
  [Key("ApplicationName")]      public string ApplicationName      { get; set; } = "";
  [Key("ApplicationVersion")]   public string ApplicationVersion   { get; set; } = "";
  [Key("ApplicationNamespace")] public string ApplicationNamespace { get; set; } = "";
  [Key("ApplicationService")]   public string ApplicationService   { get; set; } = "";
}

[MessagePackObject]
internal class WireListApplicationsResponse
{
  [Key("Applications")] public List<WireApplicationEntry> Applications { get; set; } = new();
  [Key("TotalCount")]   public long                       TotalCount   { get; set; }
}

// ── Result request messages ──────────────────────────────────────────────────

[MessagePackObject]
internal class WireCreateResultsRequest
{
  [Key("Results")] public List<WireResultData> Results { get; set; } = new();
}

[MessagePackObject]
internal class WireGetResultsRequest
{
  [Key("Filter")] public WireCoarseResultFilter Filter { get; set; } = new();
}

[MessagePackObject]
internal class WireUpdateOneResultRequest
{
  [Key("ResultId")] public string                ResultId { get; set; } = "";
  [Key("Updates")]  public List<WireFieldUpdate>  Updates  { get; set; } = new();
}

[MessagePackObject]
internal class WireUpdateManyResultsRequest
{
  [Key("Filter")]  public WireCoarseResultFilter  Filter  { get; set; } = new();
  [Key("Updates")] public List<WireFieldUpdate>   Updates { get; set; } = new();
}

[MessagePackObject]
internal class WireOwnershipTransfer
{
  [Key("ResultIds")] public List<string> ResultIds { get; set; } = new();
  [Key("NewTaskId")] public string       NewTaskId { get; set; } = "";
}

[MessagePackObject]
internal class WireChangeResultOwnershipRequest
{
  [Key("OldTaskId")]  public string                    OldTaskId  { get; set; } = "";
  [Key("Transfers")]  public List<WireOwnershipTransfer> Transfers { get; set; } = new();
}

[MessagePackObject]
internal class WireOwnershipAssignment
{
  [Key("ResultId")] public string ResultId { get; set; } = "";
  [Key("TaskId")]   public string TaskId   { get; set; } = "";
}

[MessagePackObject]
internal class WireSetTaskOwnershipRequest
{
  [Key("Assignments")] public List<WireOwnershipAssignment> Assignments { get; set; } = new();
}

[MessagePackObject]
internal class WireAddTaskDependenciesRequest
{
  [Key("Dependencies")] public Dictionary<string, List<string>> Dependencies { get; set; } = new();
}

[MessagePackObject]
internal class WireAbortTaskResultsRequest
{
  [Key("SessionId")]   public string SessionId   { get; set; } = "";
  [Key("OwnerTaskId")] public string OwnerTaskId { get; set; } = "";
}

[MessagePackObject]
internal class WireDeleteResultsRequest
{
  [Key("ResultIds")] public List<string>? ResultIds { get; set; }
  [Key("SessionId")] public string?       SessionId { get; set; }
}

// ── Session request messages ─────────────────────────────────────────────────

[MessagePackObject]
internal class WireCreateSessionRequest
{
  [Key("PartitionIds")] public List<string>    PartitionIds { get; set; } = new();
  [Key("Options")]      public WireTaskOptions Options      { get; set; } = new();
}

[MessagePackObject]
internal class WireCreateSessionResponse
{
  [Key("SessionId")] public string SessionId { get; set; } = "";
}

[MessagePackObject]
internal class WireGetSessionRequest
{
  [Key("SessionId")] public string SessionId { get; set; } = "";
}

[MessagePackObject]
internal class WireUpdateOneSessionRequest
{
  [Key("SessionId")] public string                SessionId { get; set; } = "";
  [Key("Updates")]   public List<WireFieldUpdate>  Updates   { get; set; } = new();
}

[MessagePackObject]
internal class WireFindSessionsRequest
{
  [Key("Filter")]    public WireCoarseSessionFilter Filter    { get; set; } = new();
  [Key("OrderBy")]   public string                  OrderBy   { get; set; } = "";
  [Key("Ascending")] public bool                    Ascending { get; set; }
  [Key("Page")]      public int                     Page      { get; set; }
  [Key("PageSize")]  public int                     PageSize  { get; set; }
}

[MessagePackObject]
internal class WireListSessionsResponse
{
  [Key("Sessions")]   public List<WireSessionData> Sessions   { get; set; } = new();
  [Key("TotalCount")] public long                  TotalCount { get; set; }
}

[MessagePackObject]
internal class WireDeleteSessionRequest
{
  [Key("SessionId")] public string SessionId { get; set; } = "";
}

// ── Partition request messages ───────────────────────────────────────────────

[MessagePackObject]
internal class WireCreatePartitionsRequest
{
  [Key("Partitions")] public List<WirePartitionData> Partitions { get; set; } = new();
}

[MessagePackObject]
internal class WireReadPartitionRequest
{
  [Key("PartitionId")] public string PartitionId { get; set; } = "";
}

[MessagePackObject]
internal class WireDeletePartitionRequest
{
  [Key("PartitionId")] public string PartitionId { get; set; } = "";
}

[MessagePackObject]
internal class WireArePartitionsExistingRequest
{
  [Key("PartitionIds")] public List<string> PartitionIds { get; set; } = new();
}

[MessagePackObject]
internal class WireArePartitionsExistingResponse
{
  [Key("Exists")] public Dictionary<string, bool> Exists { get; set; } = new();
}

[MessagePackObject]
internal class WireGetPartitionsWithAllocationRequest
{
  [Key("PartitionIds")] public List<string> PartitionIds { get; set; } = new();
}

[MessagePackObject]
internal class WireFindPartitionsRequest
{
  [Key("Filter")]    public WireCoarsePartitionFilter Filter    { get; set; } = new();
  [Key("OrderBy")]   public string                    OrderBy   { get; set; } = "";
  [Key("Ascending")] public bool                      Ascending { get; set; }
  [Key("Page")]      public int                       Page      { get; set; }
  [Key("PageSize")]  public int                       PageSize  { get; set; }
}

[MessagePackObject]
internal class WireListPartitionsResponse
{
  [Key("Partitions")] public List<WirePartitionData> Partitions { get; set; } = new();
  [Key("TotalCount")] public long                    TotalCount { get; set; }
}
