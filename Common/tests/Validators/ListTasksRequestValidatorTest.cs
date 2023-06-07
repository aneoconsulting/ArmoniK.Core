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

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(ListTasksRequestValidator))]
public class ListTasksRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListTasksRequest_ = new ListTasksRequest
                                {
                                  Filter = new ListTasksRequest.Types.Filter(),
                                  Sort = new ListTasksRequest.Types.Sort
                                         {
                                           Direction = SortDirection.Asc,
                                           Field = new TaskField
                                                   {
                                                     TaskSummaryField = TaskSummaryField.CreatedAt,
                                                   },
                                         },
                                  Page     = 0,
                                  PageSize = 1,
                                };

  private readonly ListTasksRequestValidator validator_ = new();
  private          ListTasksRequest?         validListTasksRequest_;

  [Test]
  public void ListTasksRequestShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validListTasksRequest_!)
                               .IsValid);

  [Test]
  public void ListTasksRequestDefaultFilterShouldFail()
  {
    validListTasksRequest_!.Filter = default;
    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }

  [Test]
  public void ListTasksRequestDefaultSortShouldFail()
  {
    validListTasksRequest_!.Sort = default;

    foreach (var error in validator_.Validate(validListTasksRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }

  [Test]
  public void ListTasksRequestMissingFieldShouldFail()
  {
    validListTasksRequest_!.Sort = new ListTasksRequest.Types.Sort
                                   {
                                     Direction = SortDirection.Desc,
                                   };
    foreach (var error in validator_.Validate(validListTasksRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }

  [Test]
  public void ListTasksRequestMissingDirectionShouldFail()
  {
    validListTasksRequest_!.Sort = new ListTasksRequest.Types.Sort
                                   {
                                     Field = new TaskField
                                             {
                                               TaskSummaryField = TaskSummaryField.SessionId,
                                             },
                                   };
    foreach (var error in validator_.Validate(validListTasksRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }

  [Test]
  public void ListTasksRequestNegativePageShouldFail()
  {
    validListTasksRequest_!.Page = -1;
    foreach (var error in validator_.Validate(validListTasksRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }

  [Test]
  public void ListTasksRequestNegativePageSizeShouldFail()
  {
    validListTasksRequest_!.PageSize = -1;
    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }

  [Test]
  public void ListTasksRequestZeroPageSizeShouldFail()
  {
    validListTasksRequest_!.PageSize = 0;
    Assert.IsFalse(validator_.Validate(validListTasksRequest_)
                             .IsValid);
  }
}
