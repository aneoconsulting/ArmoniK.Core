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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Common.Auth.Authentication;

/// <summary>
///   Interface for the authentication storage
/// </summary>
public interface IAuthenticationTable : IInitializable
{
  /// <summary>
  ///   Get the User authentication data from the database, based on the provided certificate
  /// </summary>
  /// <param name="cn">Common name of the certificate</param>
  /// <param name="fingerprint">Fingerprint of the certificate</param>
  /// <param name="cancellationToken">Cancellation token</param>
  /// <returns>User authentication data matching the provided certificate, null if not found</returns>
  public Task<UserAuthenticationResult?> GetIdentityFromCertificateAsync(string            cn,
                                                                         string            fingerprint,
                                                                         CancellationToken cancellationToken = default);

  /// <summary>
  ///   Get the User authentication data from the database, based on the id or username. If id is not null, will be used for
  ///   matching, otherwise tries to match the username
  /// </summary>
  /// <param name="id">User Id</param>
  /// <param name="username">User name</param>
  /// <param name="cancellationToken">Cancellation token</param>
  /// <returns>User authentication data matching the id, if not null, otherwise the username, null if not found</returns>
  public Task<UserAuthenticationResult?> GetIdentityFromUserAsync(string?           id,
                                                                  string?           username,
                                                                  CancellationToken cancellationToken = default);

  /// <summary>
  ///   Adds roles to the database
  /// </summary>
  /// <param name="roles">Roles to be added</param>
  public void AddRoles(IEnumerable<RoleData> roles);

  /// <summary>
  ///   Adds users to the database
  /// </summary>
  /// <param name="users">Users to be added</param>
  public void AddUsers(IEnumerable<UserData> users);

  /// <summary>
  ///   Adds certificates to the database
  /// </summary>
  /// <param name="certificates">Certificates to be added</param>
  public void AddCertificates(IEnumerable<AuthData> certificates);
}
