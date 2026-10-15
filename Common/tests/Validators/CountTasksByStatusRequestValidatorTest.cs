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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(CountTasksByStatusRequestValidator))]
public class CountTasksByStatusRequestValidatorTest
{
  private readonly CountTasksByStatusRequestValidator validator_ = new();

  private static CountTasksByStatusRequest WithCondition(FilterField condition)
    => new()
       {
         Filters = new Filters
                   {
                     Or =
                     {
                       new FiltersAnd
                       {
                         And =
                         {
                           condition,
                         },
                       },
                     },
                   },
       };

  private static FilterField SessionCondition()
    => new()
       {
         Field = new TaskField
                 {
                   TaskSummaryField = new TaskSummaryField
                                      {
                                        Field = TaskSummaryEnumField.SessionId,
                                      },
                 },
         FilterString = new FilterString
                        {
                          Value    = "session",
                          Operator = FilterStringOperator.Equal,
                        },
       };

  [Test]
  public void CountTasksByStatusRequestWithoutFilterShouldBeValid()
    => Assert.That(validator_.Validate(new CountTasksByStatusRequest())
                             .IsValid,
                   Is.True);

  [Test]
  public void CountTasksByStatusRequestWithACompleteConditionShouldBeValid()
    => Assert.That(validator_.Validate(WithCondition(SessionCondition()))
                             .IsValid,
                   Is.True);

  [Test]
  public void CountTasksByStatusRequestConditionWithoutFieldShouldFail()
  {
    var condition = SessionCondition();
    condition.Field = null;
    Assert.That(validator_.Validate(WithCondition(condition))
                          .IsValid,
                Is.False);
  }

  [Test]
  public void CountTasksByStatusRequestConditionWithEmptyFieldShouldFail()
  {
    var condition = SessionCondition();
    condition.Field = new TaskField();
    Assert.That(validator_.Validate(WithCondition(condition))
                          .IsValid,
                Is.False);
  }

  [Test]
  public void CountTasksByStatusRequestConditionWithoutValueConditionShouldFail()
  {
    var condition = SessionCondition();
    condition.ClearValueCondition();
    Assert.That(validator_.Validate(WithCondition(condition))
                          .IsValid,
                Is.False);
  }
}
