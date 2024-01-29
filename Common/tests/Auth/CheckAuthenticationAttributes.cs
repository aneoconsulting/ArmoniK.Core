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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.gRPC.Services;

using Microsoft.AspNetCore.Authorization;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Auth;

/// <summary>
///   Checks that all gRPC services and methods have the right attribute
/// </summary>
public class CheckAuthenticationAttributes
{
  /// <summary>
  ///   Lists all gRPC services
  /// </summary>
  /// <returns>List of test case data for each gRPC services</returns>
  public static List<TestCaseData> GetAllServices()
    //Get all types in the assembly
    => new(typeof(GrpcAuthService).Assembly.GetTypes()
                                  // Keep only the ones in the same namespace as the authentication service
                                  .Where(t => string.Equals(t.Namespace,
                                                            typeof(GrpcAuthService).Namespace,
                                                            StringComparison.Ordinal))
                                  // Keep only the ones whose base type comes from Protos
                                  .Where(t => t.BaseType?.Namespace?.ToUpperInvariant()
                                               .StartsWith("ArmoniK.Api.gRPC".ToUpperInvariant()) == true)
                                  // Keep only the non-nested types
                                  .Where(t => t.DeclaringType is null)
                                  .Select(t => new TestCaseData(t).SetName("{m}" + $"({t.Name})")));

  /// <summary>
  ///   List all methods of all services
  /// </summary>
  /// <returns>List of test case data for all methods of all services</returns>
  public static IEnumerable<TestCaseData> GetAllMethods()
  {
    // Get the methods of the class object
    var objectMethods = typeof(object).GetMethods()
                                      .Select(m => m.Name)
                                      .ToHashSet();
    // Get all services
    return GetAllServices()
           // Keep only the ones that need authentication
           .Where(s => !(s.Arguments[0] as Type)!.GetCustomAttributes<IgnoreAuthenticationAttribute>()
                                                 .Any())
           .SelectMany(service =>
                       {
                         // Get all methods of the base class where :
                         var serviceBaseMethods = (service.Arguments[0] as Type)!.BaseType!.GetMethods()
                                                                                 // The method is virtual (is a gRPC callable method)
                                                                                 .Where(m => m.IsVirtual)
                                                                                 .Select(m => m.Name)
                                                                                 .ToHashSet();
                         // Get all methods of the service where :
                         return (service.Arguments[0] as Type)!.GetMethods()
                                                               // The method is actually implemented by the service, is not an object class method, and is a method declared virtual by the base class
                                                               .Where(m => m.DeclaringType == service.Arguments[0] as Type && !objectMethods.Contains(m.Name) &&
                                                                           serviceBaseMethods.Contains(m.Name))
                                                               .Select(m => new TestCaseData(service.Arguments[0],
                                                                                             m).SetName("{m}" + $"({(service.Arguments[0] as Type)?.Name}, {m.Name})"));
                       })
           .ToList();
  }

  /// <summary>
  ///   Checks that either the service requires authentication, or has the IgnoreAuthentication Attribute
  /// </summary>
  /// <param name="service">Type of the service</param>
  [TestCaseSource(nameof(GetAllServices))]
  public void CheckServiceHasAuthenticationAttribute(Type service)
    => Assert.That(service.GetCustomAttributes<IgnoreAuthenticationAttribute>()
                          .Any() || service.GetCustomAttributes<AuthorizeAttribute>()
                                           .Any(),
                   "Service {0} does not have either the Authorize Attribute or the IgnoreAuthentication Attribute",
                   service.Name);

  /// <summary>
  ///   Checks that the method either RequiresAuthorization, is in a service that does not require authentication, or
  ///   IgnoreAuthorization
  /// </summary>
  /// <param name="service">Type of the service</param>
  /// <param name="method">Method information</param>
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
