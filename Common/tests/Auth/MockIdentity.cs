// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization.Permissions;

namespace ArmoniK.Core.Common.Tests.Auth;

public class MockIdentity : UserIdentity
{
  public readonly IEnumerable<MockCertificate> Certificates;

  public MockIdentity(string                       userId,
                      string                       username,
                      IEnumerable<MockCertificate> certificates,
                      IEnumerable<string>          roles,
                      IEnumerable<Permission>      permissions,
                      string?                      authenticationType)
    : base(new UserAuthenticationResult(userId,
                                        username,
                                        roles,
                                        permissions.Select(perm => perm.ToString())),
           authenticationType)
    => Certificates = certificates;

  public bool HasCertificate(string cn,
                             string fingerprint)
    => Certificates.Any(t => t.Cn == cn && t.Fingerprint == fingerprint);

  public UserAuthenticationResult ToUserAuthenticationResult()
    => new(UserId,
           UserName,
           Roles,
           Permissions.Select(perm => perm.ToString()));

  public record MockCertificate(string Cn,
                                string Fingerprint);
}
