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
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;

using Microsoft.Extensions.Logging.Abstractions;

using MysticMind.PostgresEmbed;

using Npgsql;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   Validation of the server certificate by the pool, DbUp and the replication connection,
///   against a dedicated server started with SSL.
/// </summary>
[TestFixture]
[NonParallelizable]
public class TlsTests
{
  [OneTimeSetUp]
  public void OneTimeSetUp()
  {
    certDir_ = Path.Combine(Path.GetTempPath(),
                            $"armonik-pg-tls-{Guid.NewGuid():N}");
    Directory.CreateDirectory(certDir_);

    using var ca = CreateCa("CN=ArmoniK Test CA");
    // Only a DNS name: connecting through 127.0.0.1 is a host mismatch.
    using var server = CreateServerCertificate(ca,
                                               "localhost");
    using var otherCa = CreateCa("CN=Other CA");

    caFile_ = WritePem("ca.pem",
                       ca.ExportCertificatePem());
    otherCaFile_ = WritePem("other-ca.pem",
                            otherCa.ExportCertificatePem());
    var certFile = WritePem("server.crt",
                            server.ExportCertificatePem());
    var keyFile = WritePem("server.key",
                           server.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());
    if (!OperatingSystem.IsWindows())
    {
      // PostgreSQL refuses a private key readable by other users.
      File.SetUnixFileMode(keyFile,
                           UnixFileMode.UserRead | UnixFileMode.UserWrite);
    }

    server_ = new PgServer("18.4.0",
                           PgUser,
                           Path.Combine(certDir_,
                                        "pg"),
                           pgServerParams: new Dictionary<string, string>
                                           {
                                             {
                                               "ssl", "on"
                                             },
                                             {
                                               "ssl_cert_file", certFile
                                             },
                                             {
                                               "ssl_key_file", keyFile
                                             },
                                             {
                                               "wal_level", "logical"
                                             },
                                           },
                           addLocalUserAccessPermission: true,
                           clearInstanceDirOnStop: true,
                           locale: "C");
    server_.Start();
    WaitForServer(server_.PgPort);
  }

  [OneTimeTearDown]
  public void OneTimeTearDown()
  {
    server_?.Dispose();
    Directory.Delete(certDir_,
                     true);
  }

  private const string PgUser = "postgres";

  private PgServer? server_;
  private string    certDir_     = "";
  private string    caFile_      = "";
  private string    otherCaFile_ = "";

  private Options.PostgreSQL CreateOptions(string host,
                                           string caFile,
                                           bool   allowInsecureTls = false)
    => new()
       {
         Host             = host,
         Port             = server_!.PgPort,
         User             = PgUser,
         DatabaseName     = "postgres",
         Ssl              = true,
         CAFile           = caFile,
         AllowInsecureTls = allowInsecureTls,
       };

  private static NpgsqlConnectionProvider CreateProvider(Options.PostgreSQL options)
    => new(options,
           new InitDatabase(new InitServices()),
           NullLogger<NpgsqlConnectionProvider>.Instance);

  private static async Task OpenReplicationConnection(NpgsqlConnectionProvider provider)
  {
    await using var connection = provider.CreateReplicationConnection();
    await connection.Open(CancellationToken.None)
                    .ConfigureAwait(false);
  }

  [Test]
  public async Task TrustedCaShouldConnect()
  {
    await using var provider = CreateProvider(CreateOptions("localhost",
                                                            caFile_));

    // Init connects through the pool, then runs DbUp
    await provider.Init(CancellationToken.None)
                  .ConfigureAwait(false);
    await OpenReplicationConnection(provider)
      .ConfigureAwait(false);
  }

  [Test]
  public async Task OtherCaShouldBeRejected()
  {
    await using var provider = CreateProvider(CreateOptions("localhost",
                                                            otherCaFile_));

    Assert.That(() => provider.Init(CancellationToken.None),
                Throws.InstanceOf<NpgsqlException>());
    Assert.That(() => OpenReplicationConnection(provider),
                Throws.InstanceOf<NpgsqlException>());
  }

  [Test]
  public async Task HostMismatchShouldBeRejected()
  {
    await using var provider = CreateProvider(CreateOptions("127.0.0.1",
                                                            caFile_));

    Assert.That(() => provider.Init(CancellationToken.None),
                Throws.InstanceOf<NpgsqlException>());
    Assert.That(() => OpenReplicationConnection(provider),
                Throws.InstanceOf<NpgsqlException>());
  }

  [Test]
  public async Task HostMismatchShouldBeAcceptedWithAllowInsecureTls()
  {
    await using var provider = CreateProvider(CreateOptions("127.0.0.1",
                                                            caFile_,
                                                            true));

    await provider.Init(CancellationToken.None)
                  .ConfigureAwait(false);
    await OpenReplicationConnection(provider)
      .ConfigureAwait(false);
  }

  [Test]
  public void SslWithoutCaFileShouldThrow()
    => Assert.That(() => CreateProvider(CreateOptions("localhost",
                                                      "")),
                   Throws.InstanceOf<ArgumentOutOfRangeException>());

  private string WritePem(string fileName,
                          string content)
  {
    var path = Path.Combine(certDir_,
                            fileName);
    File.WriteAllText(path,
                      content);
    return path;
  }

  private static X509Certificate2 CreateCa(string subject)
  {
    using var key = RSA.Create(2048);
    var request = new CertificateRequest(subject,
                                         key,
                                         HashAlgorithmName.SHA256,
                                         RSASignaturePadding.Pkcs1);
    request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true,
                                                                        false,
                                                                        0,
                                                                        true));
    request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign,
                                                                true));
    return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                    DateTimeOffset.UtcNow.AddYears(1));
  }

  private static X509Certificate2 CreateServerCertificate(X509Certificate2 ca,
                                                          string           dnsName)
  {
    using var key = RSA.Create(2048);
    var request = new CertificateRequest($"CN={dnsName}",
                                         key,
                                         HashAlgorithmName.SHA256,
                                         RSASignaturePadding.Pkcs1);
    var san = new SubjectAlternativeNameBuilder();
    san.AddDnsName(dnsName);
    request.CertificateExtensions.Add(san.Build());
    request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(new OidCollection
                                                                        {
                                                                          new Oid("1.3.6.1.5.5.7.3.1"), // server authentication
                                                                        },
                                                                        false));
    using var certificate = request.Create(ca,
                                           DateTimeOffset.UtcNow.AddDays(-1),
                                           DateTimeOffset.UtcNow.AddMonths(6),
                                           RandomNumberGenerator.GetBytes(16));
    return certificate.CopyWithPrivateKey(key);
  }

  private static void WaitForServer(int port)
  {
    var connectionString = $"Host=localhost;Port={port};Database=postgres;Username={PgUser};Timeout=5;Pooling=false";
    for (var i = 0; i < 60; i++)
    {
      try
      {
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        return;
      }
      catch (NpgsqlException)
      {
        Thread.Sleep(500);
      }
    }

    throw new TimeoutException("Embedded PostgreSQL server did not become ready in time");
  }
}
