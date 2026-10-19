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
using System.Diagnostics.CodeAnalysis;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   What a principal identifier stands for, given by its prefix
/// </summary>
public enum PrincipalKind
{
  /// <summary>A person or a service account named by the directory, <c>oid:</c></summary>
  DirectoryIdentity,

  /// <summary>A person or a service account named by a certificate, <c>cn:</c></summary>
  CertificateIdentity,

  /// <summary>A role carried in the credential, <c>role:</c></summary>
  Role,

  /// <summary>Every caller, <c>*</c></summary>
  Everyone,
}

/// <summary>
///   A prefixed identifier: whoever makes a request, and whoever rights are given to
/// </summary>
/// <remarks>
///   Comparison is ordinal and no case is folded.
/// </remarks>
public sealed record Principal
{
  /// <summary>Prefix of an identity named by the directory</summary>
  public const string DirectoryPrefix = "oid:";

  /// <summary>Prefix of an identity named by a certificate</summary>
  public const string CertificatePrefix = "cn:";

  /// <summary>Prefix of a role</summary>
  public const string RolePrefix = "role:";

  /// <summary>The principal standing for every caller</summary>
  public const string EveryoneValue = "*";

  private Principal(string        value,
                    PrincipalKind kind)
  {
    Value = value;
    Kind  = kind;
  }

  /// <summary>The full identifier, prefix included</summary>
  public string Value { get; }

  /// <summary>What the identifier stands for</summary>
  public PrincipalKind Kind { get; }

  /// <summary>Whether the principal names a person or a service account</summary>
  public bool IsIdentity
    => Kind is PrincipalKind.DirectoryIdentity or PrincipalKind.CertificateIdentity;

  /// <summary>The principal standing for every caller</summary>
  public static Principal Everyone { get; } = new(EveryoneValue,
                                                  PrincipalKind.Everyone);

  /// <summary>Build an identity named by the directory</summary>
  /// <param name="id">The identifier, without prefix</param>
  /// <returns>The principal</returns>
  public static Principal FromDirectory(string id)
    => Parse(DirectoryPrefix + id);

  /// <summary>Build an identity named by a certificate</summary>
  /// <param name="commonName">The common name, without prefix</param>
  /// <returns>The principal</returns>
  public static Principal FromCertificate(string commonName)
    => Parse(CertificatePrefix + commonName);

  /// <summary>Build a role</summary>
  /// <param name="role">The role name, without prefix</param>
  /// <returns>The principal</returns>
  public static Principal FromRole(string role)
    => Parse(RolePrefix + role);

  /// <summary>
  ///   Parse a prefixed identifier
  /// </summary>
  /// <param name="value">The identifier</param>
  /// <param name="principal">The principal when valid</param>
  /// <returns>Whether the identifier carries a known prefix followed by a name that reads as it compares</returns>
  public static bool TryParse(string?                            value,
                              [NotNullWhen(true)] out Principal? principal)
  {
    principal = null;

    if (value is null)
    {
      return false;
    }

    if (value == EveryoneValue)
    {
      principal = Everyone;
      return true;
    }

    // Spaces around the name would never match a credential, and invisible characters would make the grant read
    // otherwise than it applies
    if (!TrySplit(value,
                  out var kind,
                  out _,
                  out var name) || !VisibleText.IsValid(name))
    {
      return false;
    }

    principal = new Principal(value,
                              kind);
    return true;
  }

  /// <summary>
  ///   Parse a prefixed identifier
  /// </summary>
  /// <param name="value">The identifier</param>
  /// <returns>The principal</returns>
  /// <exception cref="FormatException">The identifier is not a valid principal</exception>
  public static Principal Parse(string value)
    => TryParse(value,
                out var principal)
         ? principal
         : throw new FormatException(Explain(value));

  /// <summary>
  ///   Why an identifier is not a principal
  /// </summary>
  /// <param name="value">The identifier, refused by <see cref="TryParse" /></param>
  /// <returns>A sentence naming the offending character when the prefix is known</returns>
  internal static string Explain(string? value)
    => value is not null && TrySplit(value,
                                     out _,
                                     out var prefix,
                                     out var name) && VisibleText.FindProblem(name) is
       {
       } problem
         ? $"'{value}' is not a principal: the name after '{prefix}' {problem}"
         : $"'{value}' is not a principal: expected '{DirectoryPrefix}', '{CertificatePrefix}' or '{RolePrefix}' followed by a name without surrounding spaces nor invisible characters, or '{EveryoneValue}'";

  private static bool TrySplit(string                                value,
                               out                     PrincipalKind kind,
                               [NotNullWhen(true)] out string?       prefix,
                               [NotNullWhen(true)] out string?       name)
  {
    if (value.StartsWith(DirectoryPrefix,
                         StringComparison.Ordinal))
    {
      kind   = PrincipalKind.DirectoryIdentity;
      prefix = DirectoryPrefix;
    }
    else if (value.StartsWith(CertificatePrefix,
                              StringComparison.Ordinal))
    {
      kind   = PrincipalKind.CertificateIdentity;
      prefix = CertificatePrefix;
    }
    else if (value.StartsWith(RolePrefix,
                              StringComparison.Ordinal))
    {
      kind   = PrincipalKind.Role;
      prefix = RolePrefix;
    }
    else
    {
      kind   = default;
      prefix = null;
      name   = null;
      return false;
    }

    name = value[prefix.Length..];
    return true;
  }

  /// <inheritdoc />
  public override string ToString()
    => Value;
}
