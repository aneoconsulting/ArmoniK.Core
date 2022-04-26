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
using System.Diagnostics;
using System.Text;

using Htc.Mock;
using Htc.Mock.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Samples.HtcMock.Client;

public class HtcMockClient : IDisposable
{
  private readonly GridClient               gridClient_;
  private readonly ILogger<Htc.Mock.Client> logger_;
  private          ISessionClient           sessionClient_;

  public HtcMockClient(GridClient               gridClient,
                       ILogger<Htc.Mock.Client> logger)
  {
    gridClient_    = gridClient;
    logger_        = logger;
    sessionClient_ = null;
  }

  public bool Start(RunConfiguration runConfiguration)
  {
    logger_.LogInformation("Start new run with {configuration}",
                           runConfiguration.ToString());
    var watch = Stopwatch.StartNew();

    sessionClient_ = gridClient_.CreateSession();

    var request = runConfiguration.BuildRequest(out var shape,
                                                logger_);

    var taskId = sessionClient_.SubmitTask(DataAdapter.BuildPayload(runConfiguration,
                                                                   request));

    logger_.LogInformation("Submitted root task {taskId}",
                           taskId);
    sessionClient_.WaitSubtasksCompletion(taskId)
                 .Wait();

    var result = Encoding.Default.GetString(sessionClient_.GetResult(taskId));

    logger_.LogWarning("Final result is {result}",
                       result);
    logger_.LogWarning("Expected result is 1.{result}",
                       string.Join(".",
                                   shape));

    watch.Stop();
    logger_.LogWarning("Client was executed in {time}s",
                       watch.Elapsed.TotalSeconds);

    return result.Equals($"1.{string.Join(".", shape)}");
  }

  public void Dispose()
  {
    sessionClient_?.Dispose();
  }
}
