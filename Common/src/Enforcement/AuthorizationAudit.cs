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

using ArmoniK.Core.Authorization;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Enforcement;

/// <summary>
///   Writes one record per authorization decision under a dedicated log category
/// </summary>
public class AuthorizationAudit
{
  /// <summary>
  ///   Category of the audit records
  /// </summary>
  public const string Category = "ArmoniK.Authorization.Audit";

  private readonly ILogger logger_;

  /// <summary>
  ///   Build the audit
  /// </summary>
  /// <param name="loggerFactory">Factory of loggers</param>
  public AuthorizationAudit(ILoggerFactory loggerFactory)
    => logger_ = loggerFactory.CreateLogger(Category);

  /// <summary>
  ///   Record a decision
  /// </summary>
  /// <param name="service">Service called</param>
  /// <param name="method">Method called</param>
  /// <param name="caller">The caller, null when the request carries no valid credential</param>
  /// <param name="outcome">Outcome: Allow, Forbidden, NotFound, Unauthenticated or Fault</param>
  /// <param name="required">Permissions the operation requires</param>
  /// <param name="targets">Sessions, partitions or resources the decision was taken on</param>
  /// <param name="grants">Grants that conferred the required permissions, on an allow</param>
  /// <param name="reason">Why the request was refused, or why the decision faulted</param>
  public void Record(string                    service,
                     string                    method,
                     AuthenticatedCaller?      caller,
                     string                    outcome,
                     IEnumerable<Permission>   required,
                     IEnumerable<string>       targets,
                     IEnumerable<Grant>        grants,
                     string?                   reason = null)
    => logger_.LogInformation("Authorization {Outcome} for {Caller} on {Service}.{Method} requiring {RequiredPermissions} on {Targets}: {Reason} {@Details}",
                              outcome,
                              caller?.Caller.Identity.Value ?? "",
                              service,
                              method,
                              required.Select(permission => permission.Name)
                                      .ToList(),
                              targets.ToList(),
                              reason ?? "",
                              new AuditDetails(caller?.CredentialKind.ToString() ?? "",
                                               caller?.Caller.Roles.Select(role => role.Value)
                                                     .ToList() ?? [],
                                               grants.Select(grant => new AuditGrant(grant.Id,
                                                                                     grant.Principal.Value,
                                                                                     grant.PermissionSet.Name,
                                                                                     grant.PermissionSet.Version,
                                                                                     grant.Condition?.ToString()))
                                                     .ToList()));

  /// <summary>
  ///   Record a grant or a permission set that cannot be used, and is therefore left out of every decision
  /// </summary>
  /// <param name="subject">What cannot be used</param>
  /// <param name="reason">Why</param>
  public void RecordFault(string subject,
                          string reason)
    => logger_.LogWarning("Authorization Fault on {Subject}: {Reason}",
                          subject,
                          reason);

  /// <summary>
  ///   Record a change to the grants or the permission sets
  /// </summary>
  /// <param name="actor">Who made the change, <c>config</c> for the deployment configuration</param>
  /// <param name="generation">Generation of the grants and permission sets the change belongs to</param>
  /// <param name="change">What changed: GrantCreated, GrantDeleted, PermissionSetChanged, PermissionSetDeleted</param>
  /// <param name="subject">Grant or permission set changed</param>
  /// <param name="details">Content of the grant, or old and new versions and contents of the set</param>
  public void RecordChange(string actor,
                           long   generation,
                           string change,
                           string subject,
                           object details)
    => logger_.LogInformation("Authorization change {Change} of {Subject} by {Actor} in generation {Generation} {@Details}",
                              change,
                              subject,
                              actor,
                              generation,
                              details);

  /// <summary>
  ///   Record the generation of the grants and permission sets a replica decides on from now on
  /// </summary>
  /// <param name="generation">The generation</param>
  public void RecordGeneration(long generation)
    => logger_.LogInformation("Authorization generation {Generation} in force",
                              generation);

  private sealed record AuditGrant(string  Id,
                                   string  Principal,
                                   string  PermissionSet,
                                   int     PermissionSetVersion,
                                   string? Condition);

  private sealed record AuditDetails(string                     CredentialKind,
                                     IReadOnlyList<string>      Roles,
                                     IReadOnlyList<AuditGrant>  Grants);
}
