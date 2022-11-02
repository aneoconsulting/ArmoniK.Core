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

using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(ListSessionsRequestValidator))]
public class ListSessionsRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListSessionsRequest_ = new ListSessionsRequest
                                   {
                                     Filter = new ListSessionsRequest.Types.Filter(),
                                     Sort = new ListSessionsRequest.Types.Sort
                                            {
                                              Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                                              Field     = ListSessionsRequest.Types.OrderByField.CreatedAt,
                                            },
                                     Page     = 0,
                                     PageSize = 1,
                                   };

  private readonly ListSessionsRequestValidator validator_ = new();
  private          ListSessionsRequest?         validListSessionsRequest_;

  [Test]
  public void ListSessionsRequestShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validListSessionsRequest_!)
                               .IsValid);

  [Test]
  public void ListSessionsRequestDefaultFilterShouldFail()
  {
    validListSessionsRequest_!.Filter = default;
    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListSessionsRequestDefaultSortShouldFail()
  {
    validListSessionsRequest_!.Sort = default;

    foreach (var error in validator_.Validate(validListSessionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListSessionsRequestMissingFieldShouldFail()
  {
    validListSessionsRequest_!.Sort = new ListSessionsRequest.Types.Sort
                                      {
                                        Direction = ListSessionsRequest.Types.OrderDirection.Desc,
                                      };
    foreach (var error in validator_.Validate(validListSessionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListSessionsRequestMissingDirectionShouldFail()
  {
    validListSessionsRequest_!.Sort = new ListSessionsRequest.Types.Sort
                                      {
                                        Field = ListSessionsRequest.Types.OrderByField.SessionId,
                                      };
    foreach (var error in validator_.Validate(validListSessionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListSessionsRequestNegativePageShouldFail()
  {
    validListSessionsRequest_!.Page = -1;
    foreach (var error in validator_.Validate(validListSessionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListSessionsRequestNegativePageSizeShouldFail()
  {
    validListSessionsRequest_!.PageSize = -1;
    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListSessionsZeroNegativePageSizeShouldFail()
  {
    validListSessionsRequest_!.PageSize = 0;
    Assert.IsFalse(validator_.Validate(validListSessionsRequest_)
                             .IsValid);
  }
}
