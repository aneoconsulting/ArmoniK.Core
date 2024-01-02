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

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(ListApplicationsRequestValidator))]
public class ListApplicationsRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListApplicationsRequest_ = new ListApplicationsRequest
                                       {
                                         Filters = new Filters(),
                                         Sort = new ListApplicationsRequest.Types.Sort
                                                {
                                                  Direction = SortDirection.Asc,
                                                  Fields =
                                                  {
                                                    new ApplicationField
                                                    {
                                                      ApplicationField_ = new ApplicationRawField
                                                                          {
                                                                            Field = ApplicationRawEnumField.Name,
                                                                          },
                                                    },
                                                  },
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
    validListApplicationsRequest_!.Filters = default;
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
                                            Direction = SortDirection.Desc,
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
                                            Fields =
                                            {
                                              new ApplicationField
                                              {
                                                ApplicationField_ = new ApplicationRawField
                                                                    {
                                                                      Field = ApplicationRawEnumField.Name,
                                                                    },
                                              },
                                            },
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
  public void ListApplicationsRequestZeroPageSizeShouldFail()
  {
    validListApplicationsRequest_!.PageSize = 0;
    Assert.IsFalse(validator_.Validate(validListApplicationsRequest_)
                             .IsValid);
  }
}
