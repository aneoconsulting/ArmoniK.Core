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
using System.Text;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(CreateSmallTaskRequestValidator))]
public class CreateSmallTaskRequestValidatorTest
{
  [SetUp]
  public void SetUp()
    => validCreateSmallTaskRequest_ = new CreateSmallTaskRequest
                                      {
                                        SessionId = "Session",
                                        TaskOptions = new TaskOptions
                                                      {
                                                        MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                                        MaxRetries  = 1,
                                                        Priority    = 1,
                                                        PartitionId = "Partition",
                                                      },
                                        TaskRequests =
                                        {
                                          new TaskRequest
                                          {
                                            Payload = ByteString.CopyFrom("payload",
                                                                          Encoding.ASCII),
                                          },
                                        },
                                      };

  private readonly CreateSmallTaskRequestValidator validator_ = new();
  private          CreateSmallTaskRequest?         validCreateSmallTaskRequest_;

  [Test]
  public void CompleteRequestShouldBeValid()
    => Assert.That(validator_.Validate(validCreateSmallTaskRequest_!)
                             .IsValid,
                   Is.True);

  [Test]
  public void EmptySessionShouldFail()
  {
    validCreateSmallTaskRequest_!.SessionId = string.Empty;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void BlankSessionShouldFail()
  {
    validCreateSmallTaskRequest_!.SessionId = "        ";
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void UndefinedTaskOptionShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions = null;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void UndefinedMaxDurationShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.MaxDuration = null;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void UndefinedMaxRetriesShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.MaxRetries = default;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void ZeroMaxRetryShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.MaxRetries = 0;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void NegativeMaxRetryShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.MaxRetries = -6;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }


  [Test]
  public void UndefinedOptionsShouldBeValid()
  {
    validCreateSmallTaskRequest_!.TaskOptions.Options.Clear();
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.True);
  }

  [Test]
  public void UndefinedPriorityShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.Priority = default;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void ZeroPriorityShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.Priority = 0;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void NegativePriorityShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.Priority = -6;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void TooBigPriorityShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskOptions.Priority = 300;
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void EmptyTaskRequestShouldFail()
  {
    validCreateSmallTaskRequest_!.TaskRequests.Clear();
    Assert.That(validator_.Validate(validCreateSmallTaskRequest_)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void UndefinedPayloadShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new TaskOptions
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                                PartitionId = "Partition",
                              },
                TaskRequests =
                {
                  new TaskRequest(),
                },
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.False);
  }

  [Test]
  public void EmptyPayloadShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new TaskOptions
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                                PartitionId = "Partition",
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.Empty,
                  },
                },
              };

    Assert.That(validator_.Validate(ctr)
                          .IsValid,
                Is.False);
  }
}
