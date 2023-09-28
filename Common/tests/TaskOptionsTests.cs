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
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.gRPC.Convertors;

using Google.Protobuf.Collections;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(RpcExt))]
public class TaskOptionsTests
{
  private readonly TaskOptions? options_ = null;

  private readonly Base.DataStructures.TaskOptions completeOptions_ = new(new MapField<string, string>
                                                                          {
                                                                            {
                                                                              "key1", "val1"
                                                                            },
                                                                            {
                                                                              "key2", "val2"
                                                                            },
                                                                          },
                                                                          TimeSpan.FromSeconds(1),
                                                                          5,
                                                                          1,
                                                                          "PartitionId",
                                                                          "ApplicationName",
                                                                          "ApplicationVersion",
                                                                          "ApplicationNamespace",
                                                                          "ApplicationService",
                                                                          "EngineType");


  [Test]
  public void NullTaskOptionsShouldBeEqual()
    => Assert.AreSame(null,
                      options_.ToNullableTaskOptions());

  [Test]
  public void ConversionShouldBeEqual()
    => Assert.AreEqual(completeOptions_,
                       completeOptions_.ToGrpcTaskOptions()
                                       .ToTaskOptions());
}
