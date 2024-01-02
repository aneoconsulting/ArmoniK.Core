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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Api.gRPC.V1.SortDirection;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Validators;

[TestFixture(TestOf = typeof(ListPartitionsRequestValidator))]
public class ListPartitionsRequestValidatorTest
{
  [SetUp]
  public void Setup()
    => validListPartitionsRequest_ = new ListPartitionsRequest
                                     {
                                       Filters = new Filters(),
                                       Sort = new ListPartitionsRequest.Types.Sort
                                              {
                                                Direction = SortDirection.Asc,
                                                Field = new PartitionField
                                                        {
                                                          PartitionRawField = new PartitionRawField
                                                                              {
                                                                                Field = PartitionRawEnumField.Id,
                                                                              },
                                                        },
                                              },
                                       Page     = 0,
                                       PageSize = 1,
                                     };

  private readonly ListPartitionsRequestValidator validator_ = new();
  private          ListPartitionsRequest?         validListPartitionsRequest_;

  [Test]
  public void ListPartitionsRequestShouldBeValid()
    => Assert.IsTrue(validator_.Validate(validListPartitionsRequest_!)
                               .IsValid);

  [Test]
  public void ListPartitionsRequestDefaultFilterShouldFail()
  {
    validListPartitionsRequest_!.Filters = default;
    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListPartitionsRequestDefaultSortShouldFail()
  {
    validListPartitionsRequest_!.Sort = default;

    foreach (var error in validator_.Validate(validListPartitionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListPartitionsRequestMissingFieldShouldFail()
  {
    validListPartitionsRequest_!.Sort = new ListPartitionsRequest.Types.Sort
                                        {
                                          Direction = SortDirection.Desc,
                                        };
    foreach (var error in validator_.Validate(validListPartitionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListPartitionsRequestMissingDirectionShouldFail()
  {
    validListPartitionsRequest_!.Sort = new ListPartitionsRequest.Types.Sort
                                        {
                                          Field = new PartitionField
                                                  {
                                                    PartitionRawField = new PartitionRawField
                                                                        {
                                                                          Field = PartitionRawEnumField.Id,
                                                                        },
                                                  },
                                        };
    foreach (var error in validator_.Validate(validListPartitionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListPartitionsRequestNegativePageShouldFail()
  {
    validListPartitionsRequest_!.Page = -1;
    foreach (var error in validator_.Validate(validListPartitionsRequest_)
                                    .Errors)
    {
      Console.WriteLine(error);
    }

    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListPartitionsRequestNegativePageSizeShouldFail()
  {
    validListPartitionsRequest_!.PageSize = -1;
    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }

  [Test]
  public void ListPartitionsRequestZeroPageSizeShouldFail()
  {
    validListPartitionsRequest_!.PageSize = 0;
    Assert.IsFalse(validator_.Validate(validListPartitionsRequest_)
                             .IsValid);
  }
}
