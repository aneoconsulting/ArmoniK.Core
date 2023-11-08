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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(ListResultsRequestValidator))]
public class ListResultsRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListResultsRequest_ = new ListResultsRequest
                                  {
                                    Filters = new Filters(),
                                    Sort = new ListResultsRequest.Types.Sort
                                           {
                                             Direction = SortDirection.Asc,
                                             Field = new ResultField
                                                     {
                                                       ResultRawField = new ResultRawField
                                                                        {
                                                                          Field = ResultRawEnumField.CreatedAt,
                                                                        },
                                                     },
                                           },
                                    Page     = 0,
                                    PageSize = 1,
                                  };

  private readonly ListResultsRequestValidator validator_ = new();
  private          ListResultsRequest?         validListResultsRequest_;

  [Test]
  public void ListResultsRequestShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validListResultsRequest_!)
                               .IsValid);

  [Test]
  public void ListResultsRequestDefaultFilterShouldFail()
  {
    validListResultsRequest_!.Filters = default;
    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListResultsRequestDefaultSortShouldFail()
  {
    validListResultsRequest_!.Sort = default;

    foreach (var error in validator_.Validate(validListResultsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListResultsRequestMissingFieldShouldFail()
  {
    validListResultsRequest_!.Sort = new ListResultsRequest.Types.Sort
                                     {
                                       Direction = SortDirection.Desc,
                                     };
    foreach (var error in validator_.Validate(validListResultsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListResultsRequestMissingDirectionShouldFail()
  {
    validListResultsRequest_!.Sort = new ListResultsRequest.Types.Sort
                                     {
                                       Field = new ResultField
                                               {
                                                 ResultRawField = new ResultRawField
                                                                  {
                                                                    Field = ResultRawEnumField.Name,
                                                                  },
                                               },
                                     };
    foreach (var error in validator_.Validate(validListResultsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListResultsRequestNegativePageShouldFail()
  {
    validListResultsRequest_!.Page = -1;
    foreach (var error in validator_.Validate(validListResultsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListResultsRequestNegativePageSizeShouldFail()
  {
    validListResultsRequest_!.PageSize = -1;
    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListResultsRequestZeroPageSizeShouldFail()
  {
    validListResultsRequest_!.PageSize = 0;
    Assert.IsFalse(validator_.Validate(validListResultsRequest_)
                             .IsValid);
  }
}
