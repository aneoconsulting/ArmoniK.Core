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
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Stream.Worker
{

  [PublicAPI]
  public interface ITaskHandler
  {
    /// <summary>
    /// Id of the session this task belongs to.
    /// </summary>
    string SessionId { get; }

    /// <summary>
    /// Id of the task being processed.
    /// </summary>
    string TaskId { get; }

    /// <summary>
    /// List of options provided when submitting the task.
    /// </summary>
    IReadOnlyDictionary<string, string> TaskOptions { get; }

    /// <summary>
    /// The data provided when submitting the task.
    /// </summary>
    byte[] Payload { get; }

    /// <summary>
    /// The data required to compute the task
    /// </summary>
    IReadOnlyDictionary<string, byte[]> DataDependencies { get; }

    /// <summary>
    /// Lists the result that should be provided or delegated by this task.
    /// </summary>
    IList<string> ExpectedResults { get; }

    /// <summary>
    /// The configuration parameters for the interaction with ArmoniK.
    /// </summary>
    Configuration Configuration { get; }

    /// <summary>
    /// This method allows to create subtasks.
    /// </summary>
    /// <param name="tasks">Lists the tasks to submit</param>
    /// <param name="taskOptions">The task options. If no value is provided, will use the default session options</param>
    /// <returns></returns>
    Task CreateTasksAsync(IEnumerable<TaskRequest> tasks, TaskOptions? taskOptions = null);

    /// <summary>
    /// NOT IMPLEMENTED
    /// This method is used to retrieve data available system-wide.
    /// </summary>
    /// <param name="key">The data key identifier</param>
    /// <returns></returns>
    Task<byte[]> RequestResource(string key);

    /// <summary>
    /// NOT IMPLEMENTED
    /// This method is used to retrieve data provided when creating the session.
    /// </summary>
    /// <param name="key">The da ta key identifier</param>
    /// <returns></returns>
    Task<byte[]> RequestCommonData(string key);

    /// <summary>
    /// NOT IMPLEMENTED
    /// This method is used to retrieve data directly from the submission client.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    Task<byte[]> RequestDirectData(string key);

    /// <summary>
    /// Send the results computed by the task
    /// </summary>
    /// <param name="key">The key identifier of the result.</param>
    /// <param name="data">The data corresponding to the result</param>
    /// <returns></returns>
    Task SendResult(string key, byte[] data);
  }
}
