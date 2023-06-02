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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.gRPC.Services;

using Microsoft.AspNetCore.Authorization;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Auth;

public class CheckAuthenticationIgnore
{
  public static List<TestCaseData> GetAllServices()
    => new(typeof(GrpcAuthService).Assembly.GetTypes()
                                  .Where(t => string.Equals(t.Namespace,
                                                            typeof(GrpcAuthService).Namespace,
                                                            StringComparison.Ordinal))
                                  .Where(t => t.BaseType?.Namespace?.ToUpperInvariant()
                                               .StartsWith("ArmoniK.Api.gRPC".ToUpperInvariant()) == true)
                                  .Where(t => t.DeclaringType == null)
                                  .Select(t =>
                                          {
                                            var tcd = new TestCaseData(t);
                                            tcd.SetName("{m}" + $"({t.Name})");
                                            return tcd;
                                          }));

  public static IEnumerable<TestCaseData> GetAllMethods()
  {
    var objectMethods = typeof(object).GetMethods()
                                      .Select(m => m.Name)
                                      .ToHashSet();
    return GetAllServices()
           .Where(s => !(s.Arguments[0] as Type)!.GetCustomAttributes<IgnoreAuthenticationAttribute>()
                                                 .Any())
           .SelectMany(service =>
                       {
                         var serviceBaseMethods = (service.Arguments[0] as Type)!.BaseType!.GetMethods()
                                                                                 .Where(m => m.IsVirtual)
                                                                                 .Select(m => m.Name)
                                                                                 .ToHashSet();
                         return (service.Arguments[0] as Type)!.GetMethods()
                                                               .Where(m => m.DeclaringType == service.Arguments[0] as Type && !objectMethods.Contains(m.Name) &&
                                                                           serviceBaseMethods.Contains(m.Name))
                                                               .Select(m =>
                                                                       {
                                                                         var tcd = new TestCaseData(service.Arguments[0],
                                                                                                    m);
                                                                         tcd.SetName("{m}" + $"({(service.Arguments[0] as Type)?.Name}, {m.Name})");
                                                                         return tcd;
                                                                       });
                       })
           .ToList();
  }

  [TestCaseSource(nameof(GetAllServices))]
  public void CheckServiceHasAuthenticationAttribute(Type service)
    => Assert.That(service.GetCustomAttributes<IgnoreAuthenticationAttribute>()
                          .Any() || service.GetCustomAttributes<AuthorizeAttribute>()
                                           .Any(),
                   "Service {0} does not have either the Authorize Attribute or the IgnoreAuthentication Attribute",
                   service.Name);

  [TestCaseSource(nameof(GetAllMethods))]
  public void CheckMethodHasAuthorizationAttribute(Type       service,
                                                   MethodInfo method)
    => Assert.That(method.GetCustomAttributes<IgnoreAuthorizationAttribute>()
                         .Any() || method.GetCustomAttributes<RequiresPermissionAttribute>()
                                         .Any(),
                   "Method {0} of service {1} does not have either the RequirePermission Attribute or the IgnoreAuthorization Attribute",
                   method.Name,
                   service.Name);
}
