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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Concurrent;
using System.Linq;

namespace ArmoniK.Core.Common.Auth.Authentication;

/// <summary>
///   Class used as the key for the authentication cache
/// </summary>
public sealed class AuthenticationCacheKey : IEquatable<AuthenticationCacheKey>
{
  public AuthenticationCacheKey(string  connectionId,
                                string? cn                  = "",
                                string? fingerprint         = "",
                                string? impersonateId       = "",
                                string? impersonateUsername = "")
  {
    ConnectionId        = connectionId;
    CN                  = cn                  ?? "";
    Fingerprint         = fingerprint         ?? "";
    ImpersonateId       = impersonateId       ?? "";
    ImpersonateUsername = impersonateUsername ?? "";
  }

  public string ConnectionId        { get; }
  public string CN                  { get; }
  public string Fingerprint         { get; }
  public string ImpersonateId       { get; }
  public string ImpersonateUsername { get; }

  public bool Equals(AuthenticationCacheKey? other)
  {
    if (other is null)
    {
      return false;
    }

    if (ReferenceEquals(this,
                        other))
    {
      return true;
    }

    return ConnectionId        == other.ConnectionId && CN == other.CN && Fingerprint == other.Fingerprint && ImpersonateId == other.ImpersonateId &&
           ImpersonateUsername == other.ImpersonateUsername;
  }

  public override bool Equals(object? obj)
  {
    if (obj is null)
    {
      return false;
    }

    if (ReferenceEquals(this,
                        obj))
    {
      return true;
    }

    return obj is AuthenticationCacheKey key && Equals(key);
  }

  public override int GetHashCode()
    => HashCode.Combine(ConnectionId,
                        CN,
                        Fingerprint,
                        ImpersonateId,
                        ImpersonateUsername);
}

/// <summary>
///   Class used to cache the identity of a authenticated user from the request's headers.
///   This saves precious time by not needing to ask the authentication database
/// </summary>
public class AuthenticationCache
{
  private readonly ConcurrentDictionary<AuthenticationCacheKey, UserIdentity> identityStore_;

  public AuthenticationCache()
    => identityStore_ = new ConcurrentDictionary<AuthenticationCacheKey, UserIdentity>();

  /// <summary>
  ///   Get the UserIdentity associated with the given key, null if it doesn't exist
  /// </summary>
  /// <param name="key">Key obtained from the request header</param>
  /// <returns>Identity of the user</returns>
  public virtual UserIdentity? Get(AuthenticationCacheKey key)
  {
    identityStore_.TryGetValue(key,
                               out var result);
    return result;
  }

  /// <summary>
  ///   Set the user identity associated with the given key
  /// </summary>
  /// <param name="key">Key obtained from the request header</param>
  /// <param name="identity">User identity obtained thorough</param>
  public void Set(AuthenticationCacheKey key,
                  UserIdentity           identity)
    => identityStore_[key] = identity;

  /// <summary>
  ///   Removes all the user identities associated with a connection
  ///   This is useful to reduce the size of the cache when a connection closes
  /// </summary>
  /// <param name="connectionId">Connection id to remove</param>
  public void FlushConnection(string connectionId)
  {
    foreach (var s in identityStore_.Where(kv => kv.Key.ConnectionId == connectionId)
                                    .ToList())
    {
      identityStore_.TryRemove(s.Key,
                               out _);
    }
  }

  /// <summary>
  ///   Clears all values from the cache
  /// </summary>
  public void Clear()
    => identityStore_.Clear();
}
