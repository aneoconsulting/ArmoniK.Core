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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.gRPC.Validators;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(SessionFilterValidator))]
public class SessionFilterValidatorTest
{
  private readonly SessionFilterValidator validator_ = new();

  [Test]
  public void MultipleExcludedStatusesShouldBeValid()
  {
    var sf = new SessionFilter
             {
               Excluded = new SessionFilter.Types.StatusesRequest
                          {
                            Statuses =
                            {
                              SessionStatus.Cancelled,
                              SessionStatus.Running,
                            },
                          },
             };

    Assert.IsTrue(validator_.Validate(sf)
                            .IsValid);
  }

  [Test]
  public void MultipleIncludedStatusesShouldBeValid()
  {
    var sf = new SessionFilter
             {
               Included = new SessionFilter.Types.StatusesRequest
                          {
                            Statuses =
                            {
                              SessionStatus.Cancelled,
                              SessionStatus.Running,
                            },
                          },
             };

    Assert.IsTrue(validator_.Validate(sf)
                            .IsValid);
  }
}
