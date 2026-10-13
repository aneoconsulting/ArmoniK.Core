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

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
///   Who wrote a grant or a permission set
/// </summary>
public enum AuthorizationOrigin
{
  /// <summary>The deployment configuration, which owns it and reconciles it at startup</summary>
  Config,

  /// <summary>The administration API</summary>
  Api,
}

/// <summary>
///   A stored grant
/// </summary>
/// <param name="GrantId">Identifier of the grant</param>
/// <param name="Principal">Prefixed principal the grant is for, or <c>*</c></param>
/// <param name="PermissionSet">Name of the permission set given</param>
/// <param name="Condition">Text of the condition, null for a grant without condition</param>
/// <param name="Origin">Who wrote the grant</param>
/// <param name="CreationDate">When the grant was written</param>
public record GrantData(string              GrantId,
                        string              Principal,
                        string              PermissionSet,
                        string?             Condition,
                        AuthorizationOrigin Origin,
                        DateTime            CreationDate);

/// <summary>
///   A version of a stored permission set
/// </summary>
/// <param name="Name">Name of the set</param>
/// <param name="Version">Version of the content, incremented on each change</param>
/// <param name="Permissions">Names of the permissions</param>
/// <param name="Origin">Who wrote the set</param>
public record PermissionSetData(string              Name,
                                int                 Version,
                                IList<string>       Permissions,
                                AuthorizationOrigin Origin);

/// <summary>
///   Generation of the stored grants and permission sets: how many changes began, and the last that ended
/// </summary>
/// <param name="Begun">Generation of the last change that began, 0 when none did</param>
/// <param name="Ended">Generation of the last change that ended</param>
public readonly record struct AuthorizationGeneration(long Begun,
                                                      long Ended)
{
  /// <summary>Whether no change is under way, or left unfinished by a writer that failed</summary>
  public bool Stable
    => Begun == Ended;
}

/// <summary>
///   Storage of the grants and permission sets of the access model
/// </summary>
/// <remarks>
///   A change writes several grants and sets one at a time, and is read whole or not at all: under the lock, a writer
///   begins it with <see cref="BeginChangeAsync" />, writes, then ends it with <see cref="EndChangeAsync" />. A reader
///   keeps what it read only when the generation was stable and did not move meanwhile. A writer that fails leaves
///   the change unended, so that what it wrote is not read before another change completes it.
/// </remarks>
public interface IAuthorizationTable : IInitializable
{
  /// <summary>
  ///   Every stored grant
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The grants</returns>
  IAsyncEnumerable<GrantData> ListGrantsAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   Store a new grant
  /// </summary>
  /// <param name="grant">The grant</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <exception cref="GrantAlreadyExistsException">A grant with the same identifier exists</exception>
  Task CreateGrantAsync(GrantData         grant,
                        CancellationToken cancellationToken = default);

  /// <summary>
  ///   Delete a grant
  /// </summary>
  /// <param name="grantId">Identifier of the grant</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>Whether the grant existed</returns>
  Task<bool> DeleteGrantAsync(string            grantId,
                              CancellationToken cancellationToken = default);

  /// <summary>
  ///   The current version of every stored permission set
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The permission sets</returns>
  IAsyncEnumerable<PermissionSetData> ListPermissionSetsAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   A version of a permission set, current or past
  /// </summary>
  /// <param name="name">Name of the set</param>
  /// <param name="version">Version of the set</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The version, null when it does not exist</returns>
  Task<PermissionSetData?> GetPermissionSetVersionAsync(string            name,
                                                        int               version,
                                                        CancellationToken cancellationToken = default);

  /// <summary>
  ///   Store a new version of a permission set, which becomes its current version. Past versions are kept
  /// </summary>
  /// <param name="permissionSet">The new version</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  Task SetPermissionSetAsync(PermissionSetData permissionSet,
                             CancellationToken cancellationToken = default);

  /// <summary>
  ///   Delete the current version of a permission set. Past versions are kept
  /// </summary>
  /// <param name="name">Name of the set</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>Whether the set existed</returns>
  Task<bool> DeletePermissionSetAsync(string            name,
                                      CancellationToken cancellationToken = default);

  /// <summary>
  ///   Revision of the configuration the stored grants come from, 0 when none was stored
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The revision</returns>
  Task<int> GetConfigRevisionAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   Store the revision of the configuration the stored grants come from
  /// </summary>
  /// <param name="revision">The revision</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  Task SetConfigRevisionAsync(int               revision,
                              CancellationToken cancellationToken = default);

  /// <summary>
  ///   Acquire the lock serializing changes to grants and permission sets across replicas
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The lock, released when disposed</returns>
  Task<IAsyncDisposable> LockAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   The generation of the grants and permission sets
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The generation</returns>
  Task<AuthorizationGeneration> GetGenerationAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   Begin a change, under the lock
  /// </summary>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  /// <returns>The generation of the change</returns>
  Task<long> BeginChangeAsync(CancellationToken cancellationToken = default);

  /// <summary>
  ///   End a change, unless another began since
  /// </summary>
  /// <param name="generation">Generation of the change</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method</param>
  Task EndChangeAsync(long              generation,
                      CancellationToken cancellationToken = default);
}
