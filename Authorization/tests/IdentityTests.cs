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
using System.Formats.Asn1;
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

using NUnit.Framework;

namespace ArmoniK.Core.Authorization.Tests;

[TestFixture(TestOf = typeof(TokenIdentity))]
public class TokenIdentityTests
{
  private static ClaimsPrincipal Claims(params (string Type, string Value)[] claims)
    => new(new ClaimsIdentity(claims.Select(claim => new Claim(claim.Type,
                                                               claim.Value)),
                              "test"));

  private static AuthenticatedCaller Resolve(ClaimsPrincipal principal,
                                             string          subjectClaim = "oid",
                                             string          rolesClaim   = "roles")
    => TokenIdentity.FromClaims(principal,
                                subjectClaim,
                                rolesClaim);

  [Test]
  public void TheObjectIdIsTheCallerAndRolesAreRoles()
  {
    // Access model v0.98, section 7
    var caller = Resolve(Claims(("oid", "8f2c1e94-3b7a-4d21-9c5e-0a6b8d4f1e37"),
                                ("roles", "modelling-team"),
                                ("roles", "research-users"),
                                ("aud", "api://armonik")));

    Assert.That(caller.CredentialKind,
                Is.EqualTo(CredentialKind.Token));
    Assert.That(caller.Caller.Expansion,
                Is.EqualTo(new[]
                           {
                             "oid:8f2c1e94-3b7a-4d21-9c5e-0a6b8d4f1e37",
                             "role:modelling-team",
                             "role:research-users",
                             "*",
                           }));
  }

  [Test]
  public void AConfiguredSubjectClaimIsUsed()
    => Assert.That(Resolve(Claims(("sub", "kc-uuid")),
                           "sub")
                   .Caller.Identity.Value,
                   Is.EqualTo("oid:kc-uuid"));

  [Test]
  public void AConfiguredRolesClaimIsUsed()
    => Assert.That(Resolve(Claims(("oid", "u"),
                                  ("groups", "g"),
                                  ("roles", "ignored")),
                           rolesClaim: "groups")
                   .Caller.Roles.Select(role => role.Value),
                   Is.EqualTo(new[]
                              {
                                "role:g",
                              }));

  [Test]
  public void TheUriFormOfTheObjectIdIsAccepted()
    => Assert.That(Resolve(Claims(("http://schemas.microsoft.com/identity/claims/objectidentifier", "u-9")))
                   .Caller.Identity.Value,
                   Is.EqualTo("oid:u-9"));

  [Test]
  public void TheUriFormIsNotAcceptedForOtherClaims()
    => Assert.Throws<FormatException>(() => Resolve(Claims(("http://schemas.microsoft.com/identity/claims/objectidentifier", "u-9")),
                                                    "sub"));

  [Test]
  public void SubIsNotAFallbackForTheObjectId()
    => Assert.Throws<FormatException>(() => Resolve(Claims(("sub", "pairwise"))));

  [Test]
  public void ARepeatedRoleIsOneRole()
    => Assert.That(Resolve(Claims(("oid", "u"),
                                  ("roles", "a"),
                                  ("roles", "a")))
                   .Caller.Roles,
                   Has.Count.EqualTo(1));

  [Test]
  public void AnEmptyRoleIsRejected()
    => Assert.Throws<FormatException>(() => Resolve(Claims(("oid", "u"),
                                                           ("roles", ""))));

  [Test]
  public void ATokenCannotNameARoleAsItsCaller()
    // The caller is always prefixed oid:, so a subject shaped like a role stays an identity
    => Assert.That(Resolve(Claims(("oid", "role:admins")))
                   .Caller.Identity.Kind,
                   Is.EqualTo(PrincipalKind.DirectoryIdentity));
}

[TestFixture(TestOf = typeof(CertificateIdentity))]
public class CertificateIdentityTests
{
  private static X509Certificate2 Certificate(X500DistinguishedName subject)
  {
    using var key = RSA.Create(2048);

    return new CertificateRequest(subject,
                                  key,
                                  HashAlgorithmName.SHA256,
                                  RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                                                              DateTimeOffset.UtcNow.AddDays(1));
  }

  private static X509Certificate2 Certificate(string subject)
    => Certificate(new X500DistinguishedName(subject));

  // A subject whose last RDN holds two attributes, which no .NET builder produces
  private static X500DistinguishedName SubjectWithMultiValuedRdn(string?                    singleValuedCn,
                                                                 (string Oid, string Value) first,
                                                                 (string Oid, string Value) second)
  {
    var writer = new AsnWriter(AsnEncodingRules.DER);

    using (writer.PushSequence())
    {
      if (singleValuedCn is not null)
      {
        using (writer.PushSetOf())
        using (writer.PushSequence())
        {
          writer.WriteObjectIdentifier("2.5.4.3");
          writer.WriteCharacterString(UniversalTagNumber.UTF8String,
                                      singleValuedCn);
        }
      }

      using (writer.PushSetOf())
      {
        foreach (var (oid, value) in new[]
                                     {
                                       first,
                                       second,
                                     })
        {
          using (writer.PushSequence())
          {
            writer.WriteObjectIdentifier(oid);
            writer.WriteCharacterString(UniversalTagNumber.UTF8String,
                                        value);
          }
        }
      }
    }

    return new X500DistinguishedName(writer.Encode());
  }

  [Test]
  public void TheCommonNameIsTheCallerAndUnitsAreRoles()
  {
    using var certificate = Certificate("CN=alice, OU=data-science, OU=platform, O=ANEO");

    var caller = CertificateIdentity.FromCertificate(certificate);

    Assert.That(caller.CredentialKind,
                Is.EqualTo(CredentialKind.Certificate));
    Assert.That(caller.Caller.Identity.Value,
                Is.EqualTo("cn:alice"));
    Assert.That(caller.Caller.Roles.Select(role => role.Value),
                Is.EquivalentTo(new[]
                                {
                                  "role:data-science",
                                  "role:platform",
                                }));
  }

  [Test]
  public void ARoleFromACertificateIsTheSameAsFromAToken()
    => Assert.That(CertificateIdentity.FromCertificate(Certificate("CN=svc, OU=modelling-team"))
                                      .Caller.Roles,
                   Is.EqualTo(TokenIdentity.FromClaims(new ClaimsPrincipal(new ClaimsIdentity(new[]
                                                                                              {
                                                                                                new Claim("oid",
                                                                                                          "u"),
                                                                                                new Claim("roles",
                                                                                                          "modelling-team"),
                                                                                              })),
                                                       "oid",
                                                       "roles")
                                           .Caller.Roles));

  [Test]
  public void ARepeatedUnitIsOneRole()
    => Assert.That(CertificateIdentity.FromCertificate(Certificate("CN=alice, OU=platform, OU=platform"))
                                      .Caller.Roles,
                   Has.Count.EqualTo(1));

  [Test]
  public void ACertificateCannotImpersonateADirectoryIdentity()
    => Assert.That(CertificateIdentity.FromCertificate(Certificate("CN=00000000-1111-2222-3333-444444444444"))
                                      .Caller.Identity.Value,
                   Is.EqualTo("cn:00000000-1111-2222-3333-444444444444"));

  [Test]
  public void ACommaInsideTheCommonNameSurvives()
  {
    var subject = new X500DistinguishedNameBuilder();
    subject.AddCommonName("Doe, Jane");
    subject.AddOrganizationalUnitName("platform");

    var caller = CertificateIdentity.FromCertificate(Certificate(subject.Build()));

    Assert.That(caller.Caller.Identity.Value,
                Is.EqualTo("cn:Doe, Jane"));
    Assert.That(caller.Caller.Roles.Select(role => role.Value),
                Is.EqualTo(new[]
                           {
                             "role:platform",
                           }));
  }

  [Test]
  public void AMultiValuedRdnIsSkipped()
  {
    var caller = CertificateIdentity.FromCertificate(Certificate(SubjectWithMultiValuedRdn("alice",
                                                                                           ("2.5.4.11", "a"),
                                                                                           ("2.5.4.12", "b"))));

    Assert.That(caller.Caller.Identity.Value,
                Is.EqualTo("cn:alice"));
    Assert.That(caller.Caller.Roles,
                Is.Empty);
  }

  [Test]
  public void ACommonNameOnlyInsideAMultiValuedRdnIsRejected()
    => Assert.Throws<FormatException>(() => CertificateIdentity.FromCertificate(Certificate(SubjectWithMultiValuedRdn(null,
                                                                                                                       ("2.5.4.5", "123"),
                                                                                                                       ("2.5.4.3", "J.Smith")))));

  [TestCase("OU=platform, O=ANEO")]
  [TestCase("CN=alice, CN=bob")]
  public void ASubjectWithoutExactlyOneCommonNameIsRejected(string subject)
    => Assert.Throws<FormatException>(() => CertificateIdentity.FromCertificate(Certificate(subject)));
}
