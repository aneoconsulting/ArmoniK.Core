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

using System;

namespace ArmoniK.Samples.Bench.Client;

/// <summary>
///   Represents the statistics that are extracted from the execution of the application
/// </summary>
/// <param name="ElapsedTime">Elapsed time for running the complete application</param>
/// <param name="SubmissionTime">Elapsed time for submitting the tasks</param>
/// <param name="ResultRetrievingTime">Elapsed time for retrieving the results</param>
/// <param name="TotalTasks">Total number of tasks executed in the application</param>
/// <param name="ErrorTasks">Number of tasks in error in the application</param>
/// <param name="CompletedTasks">Number of tasks completed in the application</param>
/// <param name="CanceledTasks">Number of tasks canceled in the application</param>
public record ExecutionStats(TimeSpan ElapsedTime          = default,
                             TimeSpan SubmissionTime       = default,
                             TimeSpan ResultRetrievingTime = default,
                             int      TotalTasks           = default,
                             int      ErrorTasks           = default,
                             int      CompletedTasks       = default,
                             int      CanceledTasks        = default);
