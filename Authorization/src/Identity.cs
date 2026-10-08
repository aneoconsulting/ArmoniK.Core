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
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography.X509Certificates;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   The kind of credential a caller presented
/// </summary>
public enum CredentialKind
{
  /// <summary>A token from the identity provider</summary>
  Token,

  /// <summary>A client certificate</summary>
  Certificate,
}

/// <summary>
///   A caller and the credential it was derived from
/// </summary>
/// <param name="Caller">The caller</param>
/// <param name="CredentialKind">The kind of credential presented</param>
public sealed record AuthenticatedCaller(Caller         Caller,
                                         CredentialKind CredentialKind);

/// <summary>
///   Derives a caller from the claims of a validated token
/// </summary>
public static class TokenIdentity
{
  private const string ObjectIdClaim    = "oid";
  private const string ObjectIdClaimUri = "http://schemas.microsoft.com/identity/claims/objectidentifier";

  /// <summary>
  ///   Derive a caller from a validated token
  /// </summary>
  /// <param name="principal">The claims of the token</param>
  /// <param name="subjectClaim">
  ///   Claim carrying an identifier stable across applications: <c>oid</c> for Entra ID, whose <c>sub</c> is pairwise
  ///   per application, <c>sub</c> for Keycloak
  /// </param>
  /// <param name="rolesClaim">Claim carrying the roles</param>
  /// <returns>The caller, named <c>oid:</c> with roles named <c>role:</c></returns>
  /// <exception cref="FormatException">The token carries no subject, or a value that is not a valid principal name</exception>
  public static AuthenticatedCaller FromClaims(ClaimsPrincipal principal,
                                               string          subjectClaim,
                                               string          rolesClaim)
  {
    var subject = principal.FindFirst(subjectClaim)
                           ?.Value;

    // Some pipelines map the Entra object id to its SAML-era URI
    if (string.IsNullOrEmpty(subject) && subjectClaim == ObjectIdClaim)
    {
      subject = principal.FindFirst(ObjectIdClaimUri)
                         ?.Value;
    }

    if (string.IsNullOrEmpty(subject))
    {
      throw new FormatException($"The token carries no '{subjectClaim}' claim, so there is no stable identifier for the caller");
    }

    return new AuthenticatedCaller(new Caller(Principal.FromDirectory(subject),
                                              principal.FindAll(rolesClaim)
                                                       .Select(claim => Principal.FromRole(claim.Value))),
                                   CredentialKind.Token);
  }
}

/// <summary>
///   Derives a caller from a client certificate
/// </summary>
/// <remarks>
///   The certificate is validated by the ingress, never here: this only reads it.
/// </remarks>
public static class CertificateIdentity
{
  private const string CommonNameOid         = "2.5.4.3";
  private const string OrganizationalUnitOid = "2.5.4.11";

  /// <summary>
  ///   Derive a caller from a client certificate: the common name is the caller, each organizational unit is a role
  /// </summary>
  /// <param name="certificate">The client certificate</param>
  /// <returns>The caller, named <c>cn:</c> with roles named <c>role:</c></returns>
  /// <exception cref="FormatException">The subject does not carry exactly one common name</exception>
  public static AuthenticatedCaller FromCertificate(X509Certificate2 certificate)
  {
    var commonNames = AttributeValues(certificate.SubjectName,
                                      CommonNameOid);

    if (commonNames.Count != 1)
    {
      throw new FormatException($"The client certificate subject carries {commonNames.Count} common names, exactly one is required: '{certificate.Subject}'");
    }

    return new AuthenticatedCaller(new Caller(Principal.FromCertificate(commonNames[0]),
                                              AttributeValues(certificate.SubjectName,
                                                              OrganizationalUnitOid)
                                                .Select(Principal.FromRole)),
                                   CredentialKind.Certificate);
  }

  /// <summary>
  ///   Read the distinguished name structurally rather than by splitting the subject on commas, which an escaped comma
  ///   would defeat. Multi-valued RDNs are skipped: their attributes jointly name one principal.
  /// </summary>
  private static List<string> AttributeValues(X500DistinguishedName name,
                                              string                oid)
    => name.EnumerateRelativeDistinguishedNames()
           .Where(rdn => !rdn.HasMultipleElements && rdn.GetSingleElementType()
                                                        .Value == oid)
           .Select(rdn => rdn.GetSingleElementValue())
           .OfType<string>()
           .Where(value => value.Length != 0)
           .ToList();
}
