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

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(ListResultsRequestValidator))]
public class ListResultsRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListResultsRequest_ = new ListResultsRequest
                                  {
                                    Filter = new ListResultsRequest.Types.Filter(),
                                    Sort = new ListResultsRequest.Types.Sort
                                           {
                                             Direction = ListResultsRequest.Types.OrderDirection.Asc,
                                             Field     = ListResultsRequest.Types.OrderByField.CreatedAt,
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
    validListResultsRequest_!.Filter = default;
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
                                       Direction = ListResultsRequest.Types.OrderDirection.Desc,
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
                                       Field = ListResultsRequest.Types.OrderByField.Name,
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
