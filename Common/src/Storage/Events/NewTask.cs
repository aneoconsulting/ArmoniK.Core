// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.


using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.Storage.Events;

/// <summary>
///   Represents a new task update
/// </summary>
/// <param name="SessionId">The id of the session</param>
/// <param name="TaskId">The id of the task</param>
/// <param name="OriginTaskId">The id of the task before retry (the task id if no retry)</param>
/// <param name="PayloadId">The id of the payload</param>
/// <param name="ExpectedOutputKeys">The list of the id of the data produced by the task</param>
/// <param name="DataDependencies">The list of id representing the data dependencies</param>
/// <param name="RetryOfIds">The list of task id of the previous run of the task (empty of no retry)</param>
/// <param name="Status">The status of the task</param>
public record NewTask(string              SessionId,
                      string              TaskId,
                      string              OriginTaskId,
                      string              PayloadId,
                      IEnumerable<string> ExpectedOutputKeys,
                      IEnumerable<string> DataDependencies,
                      IEnumerable<string> RetryOfIds,
                      TaskStatus          Status);
