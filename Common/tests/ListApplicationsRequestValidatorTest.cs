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

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(ListApplicationsRequestValidator))]
public class ListApplicationsRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListApplicationsRequest_ = new ListApplicationsRequest
                                       {
                                         Filter = new ListApplicationsRequest.Types.Filter(),
                                         Sort = new ListApplicationsRequest.Types.Sort
                                                {
                                                  Direction = ListApplicationsRequest.Types.OrderDirection.Asc,
                                                  Field     = ListApplicationsRequest.Types.OrderByField.Name,
                                                },
                                         Page     = 0,
                                         PageSize = 1,
                                       };

  private readonly ListApplicationsRequestValidator validator_ = new();
  private          ListApplicationsRequest?         validListApplicationsRequest_;

  [Test]
  public void ListApplicationsRequestShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validListApplicationsRequest_!)
                               .IsValid);

  [Test]
  public void ListApplicationsRequestDefaultFilterShouldFail()
  {
    validListApplicationsRequest_!.Filter = default;
    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListApplicationsRequestDefaultSortShouldFail()
  {
    validListApplicationsRequest_!.Sort = default;

    foreach (var error in validator_.Validate(validListApplicationsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListApplicationsRequestMissingFieldShouldFail()
  {
    validListApplicationsRequest_!.Sort = new ListApplicationsRequest.Types.Sort
                                          {
                                            Direction = ListApplicationsRequest.Types.OrderDirection.Desc,
                                          };
    foreach (var error in validator_.Validate(validListApplicationsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListApplicationsRequestMissingDirectionShouldFail()
  {
    validListApplicationsRequest_!.Sort = new ListApplicationsRequest.Types.Sort
                                          {
                                            Field = ListApplicationsRequest.Types.OrderByField.Name,
                                          };
    foreach (var error in validator_.Validate(validListApplicationsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListApplicationsRequestNegativePageShouldFail()
  {
    validListApplicationsRequest_!.Page = -1;
    foreach (var error in validator_.Validate(validListApplicationsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListApplicationsRequestNegativePageSizeShouldFail()
  {
    validListApplicationsRequest_!.PageSize = -1;
    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListApplicationsZeroNegativePageSizeShouldFail()
  {
    validListApplicationsRequest_!.PageSize = 0;
    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }
}
