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

using ArmoniK.Utils.DocAttribute;

namespace ArmoniK.Core.Common.Injection.Options;

/// <summary>
///   Options of the authorization of requests
/// </summary>
[ExtractDocumentation("Options for Authorization")]
public class AuthorizationOptions
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = "Authorization";

  /// <summary>
  ///   Longest <see cref="GrantCacheTtl" /> accepted: a revocation applies within twice this delay
  /// </summary>
  public static readonly TimeSpan MaxGrantCacheTtl = TimeSpan.FromDays(14);

  /// <summary>
  ///   Whether requests are authorized. When false, every request is allowed
  /// </summary>
  public bool Enabled { get; set; }

  /// <summary>
  ///   How long the grants and permission sets read from storage are kept before being read again; they are still used
  ///   while being read again, so a revocation applies within twice this delay. Above zero and at most
  ///   <see cref="MaxGrantCacheTtl" />
  /// </summary>
  public TimeSpan GrantCacheTtl { get; set; } = TimeSpan.FromSeconds(10);

  /// <summary>
  ///   Revision of the grants and permission sets of the configuration. A deployment with a lower revision than the one
  ///   stored does not change the stored grants and sets
  /// </summary>
  public int ConfigRevision { get; set; }

  /// <summary>
  ///   Permission sets defined by the deployment, the only ones: none is shipped with ArmoniK
  /// </summary>
  public List<PermissionSetOption> PermissionSets { get; set; } = new();

  /// <summary>
  ///   Grants written by the deployment
  /// </summary>
  public List<GrantOption> Grants { get; set; } = new();
}

/// <summary>
///   A permission set defined by the deployment
/// </summary>
public class PermissionSetOption
{
  /// <summary>Name of the set</summary>
  public string Name { get; set; } = "";

  /// <summary>Permissions of the set, for instance <c>session:read</c>, or <c>*</c> for every permission</summary>
  public List<string> Permissions { get; set; } = new();
}

/// <summary>
///   A grant written by the deployment
/// </summary>
public class GrantOption
{
  /// <summary>Identifier of the grant</summary>
  public string Id { get; set; } = "";

  /// <summary>Prefixed principal the grant is for, or <c>*</c></summary>
  public string Principal { get; set; } = "";

  /// <summary>Name of the permission set given</summary>
  public string PermissionSet { get; set; } = "";

  /// <summary>Condition the grant is given under, absent for none; an empty condition is refused</summary>
  public string? Condition { get; set; }
}

/// <summary>
///   Options of the identification of callers
/// </summary>
[ExtractDocumentation("Options for Identity")]
public class IdentityOptions
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = "Identity";

  /// <summary>
  ///   OpenID Connect authority validating tokens. Tokens are refused when empty
  /// </summary>
  public string Authority { get; set; } = "";

  /// <summary>
  ///   Audience a token must be issued for
  /// </summary>
  public string Audience { get; set; } = "";

  /// <summary>
  ///   Tenant a token must be issued by (<c>tid</c> claim), not checked when empty
  /// </summary>
  public string TenantId { get; set; } = "";

  /// <summary>
  ///   Claim carrying an identifier of the caller stable across applications
  /// </summary>
  public string SubjectClaim { get; set; } = "oid";

  /// <summary>
  ///   Claim carrying the roles of the caller
  /// </summary>
  public string RolesClaim { get; set; } = "roles";

  /// <summary>
  ///   Whether the authority metadata must be fetched over HTTPS
  /// </summary>
  public bool RequireHttpsMetadata { get; set; } = true;

  /// <summary>
  ///   Minimum time between two reads of the authority metadata after one failed: the delay before tokens are validated
  ///   again once an authority that could not be reached is back
  /// </summary>
  public TimeSpan MetadataRefreshInterval { get; set; } = TimeSpan.FromSeconds(30);

  /// <summary>
  ///   Whether tokens are validated. When false, the claims of a token are taken as they are, without an authority: only
  ///   for a Submitter reachable solely through a proxy writing the tokens, since anyone else can write any claims
  /// </summary>
  public bool ValidateTokens { get; set; } = true;

  /// <summary>
  ///   Header carrying the client certificate forwarded by the ingress, in the Envoy <c>X-Forwarded-Client-Cert</c>
  ///   format (text or JSON) with a single element carrying <c>Cert</c>
  /// </summary>
  public string CertificateHeader { get; set; } = "X-Forwarded-Client-Cert";
}
