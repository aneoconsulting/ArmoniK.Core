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
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(TaskOptionsValidator))]
public class TaskOptionsValidatorTest
{
  private readonly TaskOptionsValidator validator_ = new();

  [Test]
  public void UndefinedMaxDurationShouldFail()
  {
    var to = new TaskOptions
             {
               MaxRetries = 1,
               Priority   = 1,
             };

    Assert.IsFalse(validator_.Validate(to)
                            .IsValid);
  }

  [Test]
  public void UndefinedMaxRetriesShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               Priority    = 1,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void ZeroMaxRetryShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               Priority    = 1,
               MaxRetries  = 0,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void NegativeMaxRetryShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               Priority    = 1,
               MaxRetries  = -6,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void UndefinedPriorityShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               MaxRetries  = 1,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void ZeroPriorityShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               MaxRetries  = 1,
               Priority    = 0,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void NegativePriorityShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               MaxRetries  = 1,
               Priority    = -6,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void TooBigPriorityShouldFail()
  {
    var to = new TaskOptions
             {
               MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
               MaxRetries  = 1,
               Priority    = 100,
             };

    Assert.IsFalse(validator_.Validate(to)
                             .IsValid);
  }

  [Test]
  public void OnlyMaxRetryAndPriorityDefinedShouldBeValid()
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
