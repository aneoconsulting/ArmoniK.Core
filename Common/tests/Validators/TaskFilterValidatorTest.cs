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
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(TaskFilterValidator))]
public class TaskFilterValidatorTest
{
  private readonly TaskFilterValidator validator_ = new();

  [Test]
  public void MultipleExcludedStatusesShouldBeValid()
  {
    var tf = new TaskFilter
             {
               Excluded = new TaskFilter.Types.StatusesRequest
                          {
                            Statuses =
                            {
                              TaskStatus.Completed,
                              TaskStatus.Cancelled,
                            },
                          },
               Session = new TaskFilter.Types.IdsRequest
                         {
                           Ids =
                           {
                             "SessionId",
                           },
                         },
             };

    Assert.IsTrue(validator_.Validate(tf)
                            .IsValid);
  }

  [Test]
  public void MultipleIncludedStatusesShouldBeValid()
  {
    var tf = new TaskFilter
             {
               Included = new TaskFilter.Types.StatusesRequest
                          {
                            Statuses =
                            {
                              TaskStatus.Completed,
                              TaskStatus.Cancelled,
                            },
                          },
               Session = new TaskFilter.Types.IdsRequest
                         {
                           Ids =
                           {
                             "SessionId",
                           },
                         },
             };

    Assert.IsTrue(validator_.Validate(tf)
                            .IsValid);
  }

  [Test]
  public void EmptyIncludedShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Included = new TaskFilter.Types.StatusesRequest(),
             };

    Assert.IsFalse(validator_.Validate(tf)
                             .IsValid);
  }

  [Test]
  public void NoTaskIdAndNoSessionIdShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Included = new TaskFilter.Types.StatusesRequest
                          {
                            Statuses =
                            {
                              TaskStatus.Completed,
                            },
                          },
             };

    Assert.IsFalse(validator_.Validate(tf)
                             .IsValid);
  }

  [Test]
  public void EmptyExcludedShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Excluded = new TaskFilter.Types.StatusesRequest(),
             };

    Assert.IsFalse(validator_.Validate(tf)
                             .IsValid);
  }

  [Test]
  public void EmptySessionShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Session = new TaskFilter.Types.IdsRequest(),
             };

    Assert.False(validator_.Validate(tf)
                           .IsValid);
  }

  [Test]
  public void EmptyTaskShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Task = new TaskFilter.Types.IdsRequest(),
             };

    Assert.False(validator_.Validate(tf)
                           .IsValid);
  }

  // One of the two is ignored, this is the behavior expected by the OneOf token in proto file
  [Test]
  public void EmptyBothTaskAndSessionShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Task    = new TaskFilter.Types.IdsRequest(),
               Session = new TaskFilter.Types.IdsRequest(),
             };

    Assert.False(validator_.Validate(tf)
                           .IsValid);
  }

  // The empty is ignored
  [Test]
  public void TaskEmptyAndSessionShouldBeValid()
  {
    var tf = new TaskFilter
             {
               Task = new TaskFilter.Types.IdsRequest(),
               Session = new TaskFilter.Types.IdsRequest
                         {
                           Ids =
                           {
                             "test",
                           },
                         },
             };

    Console.WriteLine(tf);

    Assert.IsTrue(validator_.Validate(tf)
                            .IsValid);
  }

  [Test]
  public void TaskAndEmptyIncludedShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Task = new TaskFilter.Types.IdsRequest
                      {
                        Ids =
                        {
                          "test",
                        },
                      },
               Included = new TaskFilter.Types.StatusesRequest(),
             };


    Assert.IsFalse(validator_.Validate(tf)
                             .IsValid);
  }

  [Test]
  public void TaskAndEmptyExcludedShouldNotBeValid()
  {
    var tf = new TaskFilter
             {
               Task = new TaskFilter.Types.IdsRequest
                      {
                        Ids =
                        {
                          "test",
                        },
                      },
               Excluded = new TaskFilter.Types.StatusesRequest(),
             };


    Assert.IsFalse(validator_.Validate(tf)
                             .IsValid);
  }

  // It is valid but one is ignored
  [Test]
  public void BothIdsShouldBeValid()
  {
    var tf = new TaskFilter
             {
               Task = new TaskFilter.Types.IdsRequest
                      {
                        Ids =
                        {
                          "test",
                        },
                      },
               Session = new TaskFilter.Types.IdsRequest
                         {
                           Ids =
                           {
                             "test",
                           },
                         },
             };

    Console.WriteLine(tf);

    Assert.IsTrue(validator_.Validate(tf)
                            .IsValid);
  }

  // The empty is ignored
  [Test]
  public void TaskAndSessionEmptyShouldBeValid()
  {
    var tf = new TaskFilter
             {
               Session = new TaskFilter.Types.IdsRequest(),
               Task = new TaskFilter.Types.IdsRequest
                      {
                        Ids =
                        {
                          "test",
                        },
                      },
             };

    Assert.IsTrue(validator_.Validate(tf)
                            .IsValid);
  }
}
