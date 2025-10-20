// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(TaskOptionsValidator))]
public class TaskOptionsValidatorTest
{
  [SetUp]
  public void Setup()
    => validTaskOptions_ = new TaskOptions
                           {
                             PartitionId = "PartitionId",
                             MaxDuration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)),
                             MaxRetries  = 2,
                             Priority    = 2,
                           };

  private readonly TaskOptionsValidator sessionValidator_ = new(true);
  private readonly TaskOptionsValidator taskValidator_    = new();
  private          TaskOptions?         validTaskOptions_;

  [Test]
  public void SessionTaskOptionsShouldBeValid()
    => Assert.IsTrue(sessionValidator_.Validate(validTaskOptions_!)
                                      .IsValid);

  [Test]
  public void TaskTaskOptionsShouldBeValid()
    => Assert.IsTrue(taskValidator_.Validate(validTaskOptions_!)
                                   .IsValid);

  [Test]
  public void UndefinedSessionMaxDurationShouldFail()
  {
    validTaskOptions_!.MaxDuration = null;
    Assert.IsFalse(sessionValidator_.Validate(validTaskOptions_)
                                    .IsValid);
  }

  [Test]
  public void UndefinedTaskMaxDurationShouldBeValid()
  {
    validTaskOptions_!.MaxDuration = null;
    Assert.IsTrue(taskValidator_.Validate(validTaskOptions_)
                                .IsValid);
  }

  [Test]
  public void ZeroSessionMaxRetryShouldFail()
  {
    validTaskOptions_!.MaxRetries = 0;
    Assert.IsFalse(sessionValidator_.Validate(validTaskOptions_)
                                    .IsValid);
  }

  [Test]
  public void ZeroTaskMaxRetryShouldBeValid()
  {
    validTaskOptions_!.MaxRetries = 0;
    Assert.IsTrue(taskValidator_.Validate(validTaskOptions_)
                                .IsValid);
  }

  [Test]
  public void NegativeSessionMaxRetryShouldFail()
  {
    validTaskOptions_!.MaxRetries = -6;
    Assert.IsFalse(sessionValidator_.Validate(validTaskOptions_)
                                    .IsValid);
  }

  [Test]
  public void NegativeTaskMaxRetryShouldFail()
  {
    validTaskOptions_!.MaxRetries = -6;
    Assert.IsFalse(taskValidator_.Validate(validTaskOptions_)
                                 .IsValid);
  }

  [Test]
  public void ZeroSessionPriorityShouldFail()
  {
    validTaskOptions_!.Priority = 0;
    Assert.IsFalse(sessionValidator_.Validate(validTaskOptions_)
                                    .IsValid);
  }

  [Test]
  public void ZeroTaskPriorityShouldBeValid()
  {
    validTaskOptions_!.Priority = 0;
    Assert.IsTrue(taskValidator_.Validate(validTaskOptions_)
                                .IsValid);
  }

  [Test]
  public void NegativeSessionPriorityShouldFail()
  {
    validTaskOptions_!.Priority = -6;
    Assert.IsFalse(sessionValidator_.Validate(validTaskOptions_)
                                    .IsValid);
  }

  [Test]
  public void NegativeTaskPriorityShouldFail()
  {
    validTaskOptions_!.Priority = -6;
    Assert.IsFalse(taskValidator_.Validate(validTaskOptions_)
                                 .IsValid);
  }

  [Test]
  public void TooBigSessionPriorityShouldFail()
  {
    validTaskOptions_!.Priority = 100;
    Assert.IsFalse(sessionValidator_.Validate(validTaskOptions_)
                                    .IsValid);
  }

  [Test]
  public void TooBigTaskPriorityShouldFail()
  {
    validTaskOptions_!.Priority = 100;
    Assert.IsFalse(taskValidator_.Validate(validTaskOptions_)
                                 .IsValid);
  }

  [Test]
  public void EmptySessionPartitionShouldSucceed()
  {
    validTaskOptions_!.PartitionId = string.Empty;
    Assert.IsTrue(sessionValidator_.Validate(validTaskOptions_)
                                   .IsValid);
  }

  [Test]
  public void EmptyTaskPartitionShouldSucceed()
  {
    validTaskOptions_!.PartitionId = string.Empty;
    Assert.IsTrue(taskValidator_.Validate(validTaskOptions_)
                                .IsValid);
  }

  [Test]
  public void OnlySessionMaxRetryAndPriorityDefinedShouldFail()
  {
    var to = new TaskOptions
             {
               MaxRetries = 1,
               Priority   = 99,
             };

    Assert.IsFalse(sessionValidator_.Validate(to)
                                    .IsValid);
  }

  [Test]
  public void DefaultTaskTaskOptionsShouldBeValid()
  {
    var to = new TaskOptions();

    Assert.IsTrue(taskValidator_.Validate(to)
                                .IsValid);
  }
}
