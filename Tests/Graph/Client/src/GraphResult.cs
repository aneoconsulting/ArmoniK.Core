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

using System.Collections.Generic;

using ArmoniK.Samples.Graph.Common;

namespace ArmoniK.Samples.Graph.Client;

/// <summary>
///   All the numbers of a run, logged once at the end in the <c>GRAPH_RESULT</c> event
/// </summary>
/// <param name="SessionId">Session of the run, null if it could not be created</param>
/// <param name="Outcome">finished, failed, timeout or interrupted</param>
/// <param name="Success">Whether every task completed and the session holds exactly NTasks tasks</param>
/// <param name="NTasks">Requested number of tasks</param>
/// <param name="Settings">Graph settings, as written in the task options</param>
/// <param name="Stats">Statistics of the generated graph</param>
/// <param name="GenerationMs">Time to generate the graph and compute its statistics</param>
/// <param name="SubmissionMs">Time to submit the client graph</param>
/// <param name="WallClockMs">Time from the first submission to the end of the run</param>
/// <param name="ThroughputTasksPerSecond">Completed tasks per second of wall clock</param>
/// <param name="MinimumDurationMs">Theoretical minimum duration of the run (critical path)</param>
/// <param name="Completed">Number of completed tasks</param>
/// <param name="Errors">Number of tasks in error</param>
/// <param name="TotalCounted">Number of tasks of the session, retried ones excluded</param>
/// <param name="CountsByStatus">Number of tasks of the session by status</param>
public sealed record GraphResult(string?                     SessionId,
                                 string                      Outcome,
                                 bool                        Success,
                                 int                         NTasks,
                                 IDictionary<string, string> Settings,
                                 GraphStats                  Stats,
                                 double                      GenerationMs,
                                 double                      SubmissionMs,
                                 double                      WallClockMs,
                                 double                      ThroughputTasksPerSecond,
                                 double                      MinimumDurationMs,
                                 int                         Completed,
                                 int                         Errors,
                                 int                         TotalCounted,
                                 Dictionary<string, int>     CountsByStatus);
