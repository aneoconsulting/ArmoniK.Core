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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

using ArmoniK.Core.Common.Enforcement;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Enforcement;

[TestFixture(TestOf = typeof(ForwardedClientCertificate))]
public class ForwardedClientCertificateTest
{
  [OneTimeSetUp]
  public void SetUp()
  {
    client_ = SelfSigned("CN=client, OU=supervisors");
    other_  = SelfSigned("CN=other");
  }

  [OneTimeTearDown]
  public void TearDown()
  {
    client_.Dispose();
    other_.Dispose();
  }

  private static readonly string[] By   = ["http://frontend.lyft.com"];
  private static readonly string[] Uris = ["http://testclient.lyft.com"];
  private static readonly string[] Dns  = ["lyft.com", "www.lyft.com"];

  private X509Certificate2 client_ = null!;
  private X509Certificate2 other_  = null!;

  private static X509Certificate2 SelfSigned(string subject)
  {
    using var key = RSA.Create(2048);
    return new CertificateRequest(subject,
                                  key,
                                  HashAlgorithmName.SHA256,
                                  RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                                                              DateTimeOffset.UtcNow.AddDays(1));
  }

  private static string Pem(X509Certificate2 certificate)
    => certificate.ExportCertificatePem();

  private static string Escaped(X509Certificate2 certificate)
    => Uri.EscapeDataString(Pem(certificate));

  private static string Hash(X509Certificate2 certificate)
    => Convert.ToHexStringLower(SHA256.HashData(certificate.RawData));

  private static string Json(object value)
    => JsonSerializer.Serialize(value);

  private void AssertIsClient(string header)
  {
    using var certificate = ForwardedClientCertificate.Parse(header);
    Assert.That(certificate.RawData,
                Is.EqualTo(client_.RawData));
  }

  private static void AssertRefused(string header)
    => Assert.That(() => ForwardedClientCertificate.Parse(header),
                   Throws.InstanceOf<FormatException>());

  [Test]
  public void TextWithTheFieldsOfEnvoyIsRead()
    => AssertIsClient($"By=http://frontend.lyft.com;Hash={Hash(client_)};Cert=\"{Escaped(client_)}\";Chain=\"{Escaped(other_)}\";Subject=\"/C=US/ST=CA/L=San Francisco/OU=Lyft/CN=Test Client\";URI=http://testclient.lyft.com;DNS=lyft.com;DNS=www.lyft.com");

  [Test]
  public void TextCertificateMayBeUnquoted()
    => AssertIsClient($"Cert={Escaped(client_)}");

  [Test]
  public void TextKeysAreCaseInsensitive()
    => AssertIsClient($"hAsH={Hash(client_).ToUpperInvariant()};cErT=\"{Escaped(client_)}\"");

  [Test]
  public void TextQuotedValuesMayHoldDelimitersAndEscapedQuotes()
    => AssertIsClient($"Subject=\"CN=a,OU=b;c=\\\"d\\\"\";Cert=\"{Escaped(client_)}\"");

  [Test]
  public void JsonWithTheFieldsOfEnvoyIsRead()
    => AssertIsClient(Json(new[]
                           {
                             new
                             {
                               by   = By,
                               hash = Hash(client_),
                               cert = Pem(client_),
                               chain = new[]
                                       {
                                         Pem(other_),
                                       },
                               subject = "/C=US/ST=CA/L=San Francisco/OU=Lyft/CN=Test Client",
                               uri     = Uris,
                               dns     = Dns,
                             },
                           }));

  [Test]
  public void TheFormerRawPemIsRefused()
    => AssertRefused(Escaped(client_));

  [Test]
  public void SeveralTextElementsAreRefused()
    => AssertRefused($"Cert=\"{Escaped(other_)}\",Cert=\"{Escaped(client_)}\"");

  [Test]
  public void SeveralJsonElementsAreRefused()
    => AssertRefused(Json(new[]
                          {
                            new
                            {
                              cert = Pem(other_),
                            },
                            new
                            {
                              cert = Pem(client_),
                            },
                          }));

  [TestCase("[]")]
  [TestCase("[null]")]
  [TestCase("[{\"cert\":")]
  [TestCase("[{\"cert\":5}]")]
  [TestCase("[\"cert\"]")]
  public void MalformedJsonIsRefused(string header)
    => AssertRefused(header);

  [Test]
  public void ARepeatedJsonCertificateIsRefused()
    => AssertRefused($"[{{\"cert\":{JsonSerializer.Serialize(Pem(other_))},\"cert\":{JsonSerializer.Serialize(Pem(client_))}}}]");

  [Test]
  public void AUrlEncodedJsonCertificateIsRefused()
    => AssertRefused(Json(new[]
                          {
                            new
                            {
                              cert = Escaped(client_),
                            },
                          }));

  [Test]
  public void ARepeatedTextCertificateIsRefused()
    => AssertRefused($"Cert=\"{Escaped(other_)}\";Cert=\"{Escaped(client_)}\"");

  [Test]
  public void ARepeatedTextHashIsRefused()
    => AssertRefused($"Hash={Hash(client_)};Hash={Hash(client_)};Cert=\"{Escaped(client_)}\"");

  [Test]
  public void AChainWithoutCertificateIsRefused()
    => AssertRefused($"Hash={Hash(client_)};Chain=\"{Escaped(client_)}\"");

  [Test]
  public void AHashOfAnotherCertificateIsRefused()
    => AssertRefused($"Hash={Hash(other_)};Cert=\"{Escaped(client_)}\"");

  [Test]
  public void AJsonHashOfAnotherCertificateIsRefused()
    => AssertRefused(Json(new[]
                          {
                            new
                            {
                              hash = Hash(other_),
                              cert = Pem(client_),
                            },
                          }));

  [Test]
  public void TwoCertificatesInCertAreRefused()
    => AssertRefused($"Cert=\"{Uri.EscapeDataString(Pem(client_) + Pem(other_))}\"");

  [Test]
  public void TextAroundTheCertificateIsRefused()
    => AssertRefused($"Cert=\"{Uri.EscapeDataString("garbage" + Pem(client_))}\"");

  [Test]
  public void AnotherPemLabelIsRefused()
    => AssertRefused($"Cert=\"{Uri.EscapeDataString(Pem(client_).Replace("CERTIFICATE", "PUBLIC KEY"))}\"");

  [TestCase("")]
  [TestCase("Cert=")]
  [TestCase("Cert=\"\"")]
  [TestCase("not-a-certificate")]
  [TestCase("=value")]
  [TestCase("Cert=\"unterminated")]
  [TestCase("Cert=\"a\"b")]
  [TestCase("Cert=a=b")]
  [TestCase("Cert=a\"b")]
  [TestCase("Ce\"rt=a")]
  [TestCase("Cert=a;")]
  [TestCase("Cert=a,")]
  public void MalformedTextIsRefused(string header)
    => AssertRefused(header);
}
