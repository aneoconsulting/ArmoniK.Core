// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options.Database;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Adapters.Memory;

/// <summary>
///   In-memory implementation of <see cref="IAuthenticationTable" />.
/// </summary>
/// <remarks>
///   Every public method runs under a single lock, so that each call is atomic.
/// </remarks>
public class AuthenticationTable : IAuthenticationTable
{
  private readonly List<AuthData>            certificates_ = new();
  private readonly InitDatabase?             initDatabase_;
  private readonly Lock                      lock_  = new();
  private readonly Dictionary<int, RoleData> roles_ = new();
  private readonly Dictionary<int, UserData> users_ = new();

  private bool isInitialized_;

  /// <summary>
  ///   Creates an empty authentication table
  /// </summary>
  /// <param name="initDatabase">Roles, users and certificates to create when the table is initialized, if any</param>
  public AuthenticationTable(InitDatabase? initDatabase = null)
    => initDatabase_ = initDatabase;

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    lock (lock_)
    {
      if (!isInitialized_ && initDatabase_ is
          {
            Init: true,
          })
      {
        // Entries that already exist are kept, as the database adapters do.
        foreach (var role in initDatabase_.Roles)
        {
          roles_.TryAdd(role.RoleId,
                        role);
        }

        foreach (var user in initDatabase_.Users)
        {
          users_.TryAdd(user.UserId,
                        user);
        }

        foreach (var certificate in initDatabase_.Auths)
        {
          if (certificates_.TrueForAll(auth => auth.AuthId != certificate.AuthId))
          {
            certificates_.Add(certificate);
          }
        }
      }

      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(isInitialized_
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <inheritdoc />
  public Task<UserAuthenticationResult?> GetIdentityFromCertificateAsync(string            cn,
                                                                         string            fingerprint,
                                                                         CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      // A certificate with the exact fingerprint takes precedence over one that accepts any fingerprint (null).
      var certificate = certificates_.Find(auth => auth.Cn == cn && auth.Fingerprint == fingerprint) ??
                        certificates_.Find(auth => auth.Cn == cn && auth.Fingerprint is null);

      return Task.FromResult(certificate is not null && users_.TryGetValue(certificate.UserId,
                                                                           out var user)
                               ? ToIdentity(user)
                               : null);
    }
  }

  /// <inheritdoc />
  public Task<UserAuthenticationResult?> GetIdentityFromUserAsync(int?              id,
                                                                  string?           username,
                                                                  CancellationToken cancellationToken = default)
  {
    lock (lock_)
    {
      // Id matching has priority over the user name
      var user = id is not null
                   ? users_.GetValueOrDefault(id.Value)
                   : username is not null
                     ? users_.Values.FirstOrDefault(data => data.Username == username)
                     : null;

      return Task.FromResult(user is null
                               ? null
                               : ToIdentity(user));
    }
  }

  /// <inheritdoc />
  public void AddRoles(IEnumerable<RoleData> roles)
  {
    lock (lock_)
    {
      foreach (var role in roles)
      {
        roles_.Add(role.RoleId,
                   role);
      }
    }
  }

  /// <inheritdoc />
  public void AddUsers(IEnumerable<UserData> users)
  {
    lock (lock_)
    {
      foreach (var user in users)
      {
        users_.Add(user.UserId,
                   user);
      }
    }
  }

  /// <inheritdoc />
  public void AddCertificates(IEnumerable<AuthData> certificates)
  {
    lock (lock_)
    {
      certificates_.AddRange(certificates);
    }
  }

  /// <summary>
  ///   Build the identity of a user from its roles. Must be called under the lock.
  /// </summary>
  /// <param name="user">User</param>
  /// <returns>
  ///   The identity of the user, with the names of its existing roles and the union of their permissions
  /// </returns>
  private UserAuthenticationResult ToIdentity(UserData user)
  {
    // Roles that do not exist are ignored
    var roles = user.Roles.Where(roles_.ContainsKey)
                    .Select(roleId => roles_[roleId])
                    .ToList();

    return new UserAuthenticationResult(user.UserId,
                                        user.Username,
                                        roles.Select(role => role.RoleName)
                                             .ToList(),
                                        roles.SelectMany(role => role.Permissions)
                                             .Distinct()
                                             .ToList());
  }
}
