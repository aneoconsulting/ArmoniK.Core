// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using System;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Convertors;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Grpc;

[TestFixture(TestOf = typeof(TaskStatusExt))]
public class TaskStatusConversionTests
{
  [Test]
  public void GrpcToInternal()
  {
    foreach (var status in Enum.GetValues<TaskStatus>())
    {
      Assert.Contains(status.ToInternalStatus(),
                      Enum.GetValues<Storage.TaskStatus>());
    }
  }

  [Test]
  public void InternalToGrpc()
  {
    foreach (var status in Enum.GetValues<Storage.TaskStatus>())
    {
      Assert.Contains(status.ToGrpcStatus(),
                      Enum.GetValues<TaskStatus>());
    }
  }
}
