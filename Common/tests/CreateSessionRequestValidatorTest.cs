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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(CreateSessionRequestValidator))]
public class CreateSessionRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validCreateSessionRequest_ = new CreateSessionRequest
                                    {
                                      DefaultTaskOption = new TaskOptions
                                                          {
                                                            Priority    = 1,
                                                            MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(2)),
                                                            MaxRetries  = 2,
                                                            PartitionId = "PartitionId",
                                                          },
                                      PartitionIds =
                                      {
                                        "PartitionId",
                                      },
                                    };

  private readonly CreateSessionRequestValidator validator_ = new();

  private CreateSessionRequest? validCreateSessionRequest_;

  [Test]
  public void NullDefaultTaskOptionShouldFail()
  {
    validCreateSessionRequest_!.DefaultTaskOption = null;
    Assert.IsFalse(validator_.Validate(validCreateSessionRequest_)
                             .IsValid);
  }


  [Test]
  public void EmptyPartitionIdInTaskOptionsShouldSucceed()
  {
    validCreateSessionRequest_!.DefaultTaskOption.PartitionId = string.Empty;
    Assert.IsTrue(validator_.Validate(validCreateSessionRequest_)
                            .IsValid);
  }


  [Test]
  public void EmptyPartitionIdShouldSucceed()
  {
    validCreateSessionRequest_!.PartitionIds.Clear();
    Assert.IsTrue(validator_.Validate(validCreateSessionRequest_)
                            .IsValid);
  }

  [Test]
  public void SessionShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validCreateSessionRequest_!)
                               .IsValid);
}
