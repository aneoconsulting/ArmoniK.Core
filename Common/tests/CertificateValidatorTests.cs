// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.IO;
using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class CertificateValidatorTests
{
  private (X509Certificate2Collection authority, X509Certificate2 req) GenCertificate()
  {
    var authority = new X509Certificate2Collection();

    var root = ECDsa.Create();
    var req = new CertificateRequest("cn=Root",
                                     root,
                                     HashAlgorithmName.SHA256);
    req.CertificateExtensions.Add(new X509BasicConstraintsExtension(true,
                                                                    false,
                                                                    0,
                                                                    true));
    var ca = req.CreateSelfSigned(DateTimeOffset.Now,
                                  DateTimeOffset.Now.AddYears(5));
    authority.Add(ca);


    var key = ECDsa.Create();
    var req2 = new CertificateRequest("cn=Child",
                                      key,
                                      HashAlgorithmName.SHA256).Create(ca,
                                                                       DateTimeOffset.Now,
                                                                       DateTimeOffset.Now.AddYears(1),
                                                                       Encoding.UTF8.GetBytes("serial"));

    return (authority, req2);
  }


  [Test]
  public void CreateCertificateValidator()
  {
    var logger = new LoggerInit(new ConfigurationManager()).GetLogger();

    var (authority, req) = GenCertificate();

    var chain = new X509Chain();
    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

    var validator = CertificateValidator.ValidationCallback(logger,
                                                            authority,
                                                            false);
    Assert.IsNotNull(validator);

    var validation = validator(this,
                               req,
                               chain,
                               SslPolicyErrors.None);

    Assert.IsTrue(validation);
  }

  private static X509Certificate2 CreateSelfSignedCert(string subject = "CN=localhost")
  {
    using var rsa = RSA.Create(2048);

    var request = new CertificateRequest(subject,
                                         rsa,
                                         HashAlgorithmName.SHA256,
                                         RSASignaturePadding.Pkcs1);

    return request.CreateSelfSigned(DateTimeOffset.Now.AddDays(-1),
                                    DateTimeOffset.Now.AddDays(365));
  }

  [Test] // ok
  public void ValidationCallback_Should_ReturnFalse_When_CertificateNull()
  {
    var mock   = new Mock<ILogger>();
    var logger = mock.Object;

    var cb = CertificateValidator.ValidationCallback(logger,
                                                     new X509Certificate2Collection(),
                                                     false);

    var result = cb(this,
                    null,
                    null,
                    SslPolicyErrors.None);

    mock.VerifyLog(LogLevel.Warning,
                   "Certificate or certificate chain is null",
                   Times.Once());
    Assert.IsFalse(result);
  }

  [Test] // ok
  public void ValidationCallback_Should_Reject_HostMismatch_When_NotAllowed()
  {
    var mock   = new Mock<ILogger>();
    var logger = mock.Object;

    var (authority, cert) = GenCertificate();

    var cb = CertificateValidator.ValidationCallback(logger,
                                                     authority,
                                                     false);

    var chain = new X509Chain();
    var result = cb(this,
                    cert,
                    chain,
                    SslPolicyErrors.RemoteCertificateNameMismatch);

    mock.VerifyLog(LogLevel.Warning,
                   "SSL validation failed: certificate name mismatch",
                   Times.Once());
    Assert.IsFalse(result);
  }

  [Test] // ok
  public void ValidationCallback_Should_Allow_HostMismatch_When_Allowed()
  {
    var mock   = new Mock<ILogger>();
    var logger = mock.Object;

    var (authority, cert) = GenCertificate();

    var cb = CertificateValidator.ValidationCallback(logger,
                                                     authority,
                                                     true);

    var chain = new X509Chain();
    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
    var result = cb(this,
                    cert,
                    chain,
                    SslPolicyErrors.RemoteCertificateNameMismatch | SslPolicyErrors.RemoteCertificateChainErrors);

    Assert.IsTrue(result);
    mock.VerifyLog(LogLevel.Debug,
                   "SSL validation succeeded: certificate is trusted",
                   Times.Once());
  }

  [Test] // ok
  public void ValidationCallback_Should_Reject_PolicyErrors_OtherThanChainErrors()
  {
    var mock   = new Mock<ILogger>();
    var logger = mock.Object;

    var cert  = CreateSelfSignedCert();
    var chain = new X509Chain();

    var cb = CertificateValidator.ValidationCallback(logger,
                                                     new X509Certificate2Collection(),
                                                     false);

    var result = cb(this,
                    cert,
                    chain,
                    SslPolicyErrors.RemoteCertificateNotAvailable);

    mock.VerifyLog(LogLevel.Warning,
                   "SSL validation failed with errors:",
                   Times.Once());
    Assert.IsFalse(result);
  }


  [Test]
  public void ValidationCallback_Should_Trust_Certificate_When_In_Authority()
  {
    var mock   = new Mock<ILogger>();
    var logger = mock.Object;

    var authorityCert = CreateSelfSignedCert();
    var presentedCert = authorityCert; // trusted

    var chain = new X509Chain();
    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

    var cb = CertificateValidator.ValidationCallback(logger,
                                                     new X509Certificate2Collection(authorityCert),
                                                     false);

    var result = cb(this,
                    presentedCert,
                    chain,
                    SslPolicyErrors.RemoteCertificateChainErrors);

    Assert.IsTrue(result);
    mock.VerifyLog(LogLevel.Debug,
                   "SSL validation succeeded: certificate is trusted",
                   Times.Once());
  }

  [Test]
  public void ValidationCallback_Should_Reject_When_Not_In_Authority()
  {
    var mock   = new Mock<ILogger>();
    var logger = mock.Object;

    var authorityCert = CreateSelfSignedCert("CN=trustedCA");
    var presentedCert = CreateSelfSignedCert("CN=untrusted");

    var chain = new X509Chain();
    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;

    var cb = CertificateValidator.ValidationCallback(logger,
                                                     new X509Certificate2Collection(authorityCert),
                                                     false);

    var result = cb(this,
                    presentedCert,
                    chain,
                    SslPolicyErrors.RemoteCertificateChainErrors);

    mock.VerifyLog(LogLevel.Warning,
                   "SSL validation failed: certificate is not trusted",
                   Times.Once());
    Assert.IsFalse(result);
  }

  [Test]
  public void CreateCallback_Should_Throw_When_File_Not_Found()
  {
    var logger = new LoggerInit(new ConfigurationManager()).GetLogger();

    Assert.Throws<FileNotFoundException>(() => CertificateValidator.CreateCallback("does_not_exist.pem",
                                                                                   false,
                                                                                   logger));
  }

  [Test]
  public void CreateCallback_Should_Load_CA_And_Create_Delegate()
  {
    var logger = new LoggerInit(new ConfigurationManager()).GetLogger();

    var cert = CreateSelfSignedCert();
    var pem  = cert.ExportCertificatePem();

    var tempFile = Path.GetTempFileName();
    File.WriteAllText(tempFile,
                      pem);

    var cb = CertificateValidator.CreateCallback(tempFile,
                                                 false,
                                                 logger);

    Assert.NotNull(cb);
  }
}

public static class LoggerTestExtensions
{
  public static void VerifyLog(this Mock<ILogger> logger,
                               LogLevel           level,
                               string             expectedMessageSubstring,
                               Times              times)
    => logger.Verify(x => x.Log(It.Is<LogLevel>(l => l == level),
                                It.IsAny<EventId>(),
                                It.Is<It.IsAnyType>((state,
                                                     _) => state.ToString()!.Contains(expectedMessageSubstring)),
                                It.IsAny<Exception>(),
                                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                     times);
}
