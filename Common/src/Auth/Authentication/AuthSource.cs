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
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Auth.Authentication
{
  public class AuthSource : IAuthenticationSource
  {
    private readonly ILogger          logger_;
    private readonly IPermissionTable permissionTable_;
    public AuthSource(ConfigurationManager config,
                      IPermissionTable permissionTable,
                      ILogger              logger)
    {
      logger_ = logger;
      permissionTable_ = permissionTable;
    }

    public ClaimsIdentity? GetIdentity(string cn,
                                       string fingerprint)
    {

      return GetIdentityAsync(cn,
                              fingerprint,
                              new CancellationToken(false))
        .Result;

    }

    public async Task<ClaimsIdentity?> GetIdentityAsync(string cn,
                                       string fingerprint, CancellationToken cancellationToken)
    {

      var user = await permissionTable_.GetUserAsync(cn,
                                                      fingerprint,
                                                      cancellationToken).ConfigureAwait(false);
      if (user == null)
      {
        return null;
      }

      var claims = await permissionTable_.GetClaimsAsync(user,
                                                         cancellationToken).ConfigureAwait(false);

      return new ClaimsIdentity(claims, nameof(AuthSource));

    }
  }
}
