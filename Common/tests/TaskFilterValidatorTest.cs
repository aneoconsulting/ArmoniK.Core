// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Text;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

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
          TaskStatus.Canceled,
        },
      },
      Dispatch = new TaskFilter.Types.IdsRequest
      {
        Ids =
        {
          "DispatchId",
        },
      },
    };

    Assert.IsTrue(validator_.Validate(tf).IsValid);
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
          TaskStatus.Canceled,
        },
      },
      Dispatch = new TaskFilter.Types.IdsRequest
      {
        Ids =
        {
          "DispatchId",
        },
      },
    };

    Assert.IsTrue(validator_.Validate(tf).IsValid);
  }
}