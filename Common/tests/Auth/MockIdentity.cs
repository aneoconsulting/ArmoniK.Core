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

using System.Collections.Generic;
using System.Linq;

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;

using DnsClient.Protocol;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Tests.Auth;


public class MockIdentity : UserIdentity
{
  public record MockCertificate(string CN,
                                string Fingerprint);
  public readonly IEnumerable<MockCertificate> Certificates;
  public MockIdentity(string                              userId,
                      string                              username,
                      IEnumerable<MockCertificate>        certificates,
                      IEnumerable<string>                 roles,
                      IEnumerable<Permissions.Permission> permissions, 
                      [CanBeNull] string                  authenticationType) : base(new UserAuthenticationResult(userId, username, roles, permissions.Select(perm=>perm.ToString())), authenticationType)
  {
    Certificates = certificates;
  }

  public bool HasCertificate(string cn,
                                 string fingerprint)
    => Certificates.Any(t => t.CN == cn && t.Fingerprint == fingerprint);

  public UserAuthenticationResult ToUserAuthenticationResult()
  {
    return new UserAuthenticationResult(UserId,
                                        UserName,
                                        Roles,
                                        Permissions.Select(perm => perm.ToString()));
  }

}
