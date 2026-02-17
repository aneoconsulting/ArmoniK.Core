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

using System;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(CreateLargeTaskRequestValidator))]
public class CreateLargeTaskRequestValidatorTest
{
  private readonly CreateLargeTaskRequestValidator validator_ = new();

  [Test]
  public void CompleteInitRequestShouldBeValid()
  {
    var ctr = new CreateLargeTaskRequest
              {
                InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                              {
                                TaskOptions = new TaskOptions
                                              {
                                                MaxDuration = Duration.FromTimeSpan(TimeSpan.Zero),
                                                MaxRetries  = 3,
                                                Priority    = 1,
                                                PartitionId = "Partition",
                                              },
                                SessionId = "SessionId",
                              },
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.True);
  }

  [Test]
  public void MissingSessionIdShouldFail()
  {
    var ctr = new CreateLargeTaskRequest
              {
                InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                              {
                                TaskOptions = new TaskOptions
                                              {
                                                MaxDuration = Duration.FromTimeSpan(TimeSpan.Zero),
                                                MaxRetries  = 3,
                                                Priority    = 1,
                                                PartitionId = "Partition",
                                              },
                              },
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void BlankSessionShouldFail()
  {
    var ctr = new CreateLargeTaskRequest
              {
                InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                              {
                                TaskOptions = new TaskOptions
                                              {
                                                MaxDuration = Duration.FromTimeSpan(TimeSpan.Zero),
                                                MaxRetries  = 3,
                                                Priority    = 1,
                                                PartitionId = "Partition",
                                              },
                                SessionId = "    ",
                              },
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void NullPayloadShouldFail()
  {
    var ctr = new CreateLargeTaskRequest
              {
                TaskPayload = null,
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void EmptyDataChunkShouldFail()
  {
    var ctr = new CreateLargeTaskRequest
              {
                TaskPayload = new DataChunk(),
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void NullDataInDataChunkShouldThrowError()
    => Assert.Throws<ArgumentNullException>(() => new CreateLargeTaskRequest
                                                  {
                                                    TaskPayload = new DataChunk
                                                                  {
                                                                    Data = null,
                                                                  },
                                                  });

  [Test]
  public void EmptyDataInDataChunkShouldSucceed()
  {
    var ctr = new CreateLargeTaskRequest
              {
                TaskPayload = new DataChunk
                              {
                                Data = ByteString.Empty,
                              },
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.True);
  }
}
