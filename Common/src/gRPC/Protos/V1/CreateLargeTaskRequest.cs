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

using Google.Protobuf.Collections;

using Grpc.Core;

using Microsoft.Extensions.Logging;

using static ArmoniK.Api.gRPC.V1.CreateLargeTaskRequest.Types;

// ReSharper disable once CheckNamespace
namespace ArmoniK.Api.gRPC.V1;

public sealed partial class CreateLargeTaskRequest
{
  private void CheckCase(TypeOneofCase oneOfCase, ILogger logger, string errorDetails)
  {
    if (TypeCase != oneOfCase)
    {
      var exception = new RpcException(new(StatusCode.InvalidArgument,
                                           $"Expected a stream message of type {oneOfCase}: {errorDetails}"));
      logger.LogError(exception,
                      "Invalid stream");
      throw exception;
    }
  }

  public InitRequest GetInitRequest(ILogger logger, string errorDetails)
  {
    logger.LogDebug("reading CreateLargeTaskRequest.{case}",
                    TypeOneofCase.InitRequest);
    CheckCase(TypeOneofCase.InitRequest, logger, errorDetails);
    return InitRequest;
  }

  public InitTaskRequest GetInitTask(ILogger logger, string errorDetails)
  {
    logger.LogDebug("reading CreateLargeTaskRequest.{case}",
                    TypeOneofCase.InitTask);
    CheckCase(TypeOneofCase.InitTask, logger, errorDetails);
    return InitTask;
  }

  public DataChunk GetTaskPayload(ILogger logger, string errorDetails)
  {
    logger.LogDebug("reading CreateLargeTaskRequest.{case}",
                    TypeOneofCase.TaskPayload);
    CheckCase(TypeOneofCase.TaskPayload, logger, errorDetails);
    return TaskPayload;
  }


}
