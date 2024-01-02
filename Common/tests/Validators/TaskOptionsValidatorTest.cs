// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

  private readonly TaskOptionsValidator validator_ = new();
  private          TaskOptions?         validTaskOptions_;

  [Test]
  public void TaskOptionsShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validTaskOptions_!)
                               .IsValid);

  [Test]
  public void UndefinedMaxDurationShouldFail()
  {
    validTaskOptions_!.MaxDuration = null;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void UndefinedMaxRetriesShouldFail()
  {
    validTaskOptions_!.MaxRetries = default;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void ZeroMaxRetryShouldFail()
  {
    validTaskOptions_!.MaxRetries = 0;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void NegativeMaxRetryShouldFail()
  {
    validTaskOptions_!.MaxRetries = -6;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void UndefinedPriorityShouldFail()
  {
    validTaskOptions_!.Priority = default;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void ZeroPriorityShouldFail()
  {
    validTaskOptions_!.Priority = 0;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void NegativePriorityShouldFail()
  {
    validTaskOptions_!.Priority = -6;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void TooBigPriorityShouldFail()
  {
    validTaskOptions_!.Priority = 100;
    Assert.IsFalse(validator_.Validate(validTaskOptions_)
                             .IsValid);
  }

  [Test]
  public void EmptyPartitionShouldSucceed()
  {
    validTaskOptions_!.PartitionId = string.Empty;
    Assert.IsTrue(validator_.Validate(validTaskOptions_)
                            .IsValid);
  }

  [Test]
  public void OnlyMaxRetryAndPriorityDefinedShouldFail()
  {
    var to = new TaskOptions
             {
               MaxRetries = 1,
               Priority   = 100,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }
}
