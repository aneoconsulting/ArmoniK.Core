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

using System.Collections.Generic;
using System.Linq;

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.gRPC;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(RpcExt))]
public class ApplicationRawTests
{
  private const string Name    = "name";
  private const string Ns      = "namespace";
  private const string Version = "version";
  private const string Service = "service";

  private readonly ApplicationRaw app1_ = new()
                                          {
                                            Name      = Name,
                                            Namespace = Ns,
                                            Version   = Version,
                                            Service   = Service,
                                          };

  private readonly ApplicationRaw app2_ = new()
                                          {
                                            Name      = Name,
                                            Namespace = Ns,
                                            Version   = Version,
                                            Service   = Service,
                                          };

  [Test]
  public void ApplicationRawShouldBeEquals()
    => Assert.AreEqual(app1_,
                       app2_);

  [Test]
  public void DistinctShouldWork()
  {
    var list = new List<ApplicationRaw>
               {
                 app1_,
                 app2_,
               };

    Assert.AreEqual(1,
                    list.Distinct()
                        .Count());
  }
}
