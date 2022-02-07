// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1;

namespace ArmoniK.Core.Common.Storage;

public record TaskData(string        SessionId,
                       string        ParentTaskId,
                       string        DispatchId,
                       string        TaskId,
                       IList<string> DataDependencies,
                       IList<string> ExpectedOutput,
                       bool          HasPayload,
                       byte[]        Payload,
                       TaskStatus    Status,
                       TaskOptions   Options,
                       IList<string> AncestorDispatchIds,
                       DateTime      CreationDate)
{
  public TaskData(string        sessionId,
                  string        parentTaskId,
                  string        dispatchId,
                  string        taskId,
                  IList<string> dataDependencies,
                  IList<string> expectedOutput,
                  bool          hasPayload,
                  byte[]        payload,
                  TaskStatus    status,
                  TaskOptions   options,
                  IList<string> ancestorDispatchIds)
    : this(sessionId,
           parentTaskId,
           dispatchId,
           taskId,
           dataDependencies,
           expectedOutput,
           hasPayload,
           payload,
           status,
           options,
           ancestorDispatchIds,
           DateTime.UtcNow)
  {
  }
}