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

using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Broker.Tests;

/// <summary>
///   Server certificate validation against a private CA, through the shared Core validator.
/// </summary>
[TestFixture]
[NonParallelizable]
public class BrokerTlsTests
{
  [SetUp]
  public void SetUp()
  {
    binary_ = BrokerIntegrationTests.FindBinary();
    if (binary_ is null)
    {
      Assert.Ignore("armonik-broker binary not built");
    }

    dir_ = Directory.CreateTempSubdirectory("broker-tls-")
                    .FullName;
    using var caKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    var caRequest = new CertificateRequest("CN=broker-test-ca",
                                           caKey,
                                           HashAlgorithmName.SHA256);
    caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true,
                                                                          false,
                                                                          0,
                                                                          true));
    caRequest.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign,
                                                                  true));
    ca_ = caRequest.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                     DateTimeOffset.UtcNow.AddDays(1));
    File.WriteAllText(Path.Combine(dir_,
                                   "ca.pem"),
                      ca_.ExportCertificatePem());
  }

  [TearDown]
  public void TearDown()
  {
    if (server_ is { HasExited: false })
    {
      server_.Kill();
      server_.WaitForExit();
    }

    server_?.Dispose();
    ca_?.Dispose();
    if (dir_ is not null)
    {
      Directory.Delete(dir_,
                       true);
    }
  }

  private string?           binary_;
  private string?           dir_;
  private X509Certificate2? ca_;
  private Process?          server_;

  /// <summary>
  ///   Issues a server certificate for <paramref name="host" /> and starts the broker with it.
  /// </summary>
  private async Task<int> StartServer(string host)
  {
    using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
    var request = new CertificateRequest($"CN={host}",
                                         key,
                                         HashAlgorithmName.SHA256);
    var san = new SubjectAlternativeNameBuilder();
    san.AddDnsName(host);
    request.CertificateExtensions.Add(san.Build());
    request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")],
                                                                        false));
    using var cert = request.Create(ca_!,
                                    DateTimeOffset.UtcNow.AddHours(-1),
                                    DateTimeOffset.UtcNow.AddHours(1),
                                    RandomNumberGenerator.GetBytes(16));
    var certFile = Path.Combine(dir_!,
                                "server.pem");
    var keyFile = Path.Combine(dir_!,
                               "server.key");
    File.WriteAllText(certFile,
                      cert.ExportCertificatePem());
    File.WriteAllText(keyFile,
                      key.ExportPkcs8PrivateKeyPem());

    var port = BrokerIntegrationTests.FreePort();
    var info = new ProcessStartInfo(binary_!)
               {
                 RedirectStandardOutput = true,
                 RedirectStandardError  = true,
               };
    info.Environment["BROKER_LISTEN"]   = $"127.0.0.1:{port}";
    info.Environment["BROKER_TLS_CERT"] = certFile;
    info.Environment["BROKER_TLS_KEY"]  = keyFile;
    server_ = Process.Start(info)!;
    server_.BeginOutputReadLine();
    server_.BeginErrorReadLine();
    for (var i = 0; i < 100; i++)
    {
      if (server_.HasExited)
      {
        Assert.Fail("broker exited at startup");
      }

      try
      {
        using var tcp = new System.Net.Sockets.TcpClient();
        await tcp.ConnectAsync(IPAddress.Loopback,
                               port);
        return port;
      }
      catch (System.Net.Sockets.SocketException)
      {
        await Task.Delay(50);
      }
    }

    Assert.Fail("broker did not start");
    return 0;
  }

  private async Task<bool> Healthy(int  port,
                                   bool allowHostMismatch = false)
  {
    await using var client = new BrokerClient(new Broker
                                                 {
                                                   Endpoint          = $"https://localhost:{port}",
                                                   CaFile            = Path.Combine(dir_!,
                                                                                    "ca.pem"),
                                                   AllowHostMismatch = allowHostMismatch,
                                                 },
                                                 NullLogger.Instance);
    return await client.IsHealthyAsync(CancellationToken.None);
  }

  [Test]
  public async Task CertificateOfThePrivateCaIsAccepted()
    => Assert.That(await Healthy(await StartServer("localhost")),
                   Is.True);

  [Test]
  public async Task CertificateForAnotherHostIsRejected()
    => Assert.That(await Healthy(await StartServer("not-the-broker.example")),
                   Is.False,
                   "a certificate of the same CA issued for another host must not be taken for the broker");

  [Test]
  public async Task HostMismatchCanBeAllowedExplicitly()
    => Assert.That(await Healthy(await StartServer("not-the-broker.example"),
                                 true),
                   Is.True);
}
