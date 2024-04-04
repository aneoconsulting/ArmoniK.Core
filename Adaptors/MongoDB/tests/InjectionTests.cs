// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class InjectionTests
{
  [SetUp]
  public void SetUp()
  {
    var certReq = new CertificateRequest(new X500DistinguishedName("CN=test"),
                                         RSA.Create(2048),
                                         HashAlgorithmName.SHA256,
                                         RSASignaturePadding.Pkcs1);

    var cert0 = certReq.CreateSelfSigned(DateTimeOffset.UtcNow,
                                         DateTimeOffset.Now.AddDays(10));

    var path0 = Path.Combine(Path.GetTempPath(),
                             "file0.pem");
    var path1 = Path.Combine(Path.GetTempPath(),
                             "file1.pem");
    File.WriteAllText(path0,
                      cert0.ExportCertificatePem() + "\n" + cert0.GetRSAPrivateKey()!.ExportRSAPrivateKeyPem());
    File.WriteAllText(path1,
                      cert0.ExportCertificatePem() + "\n" + cert0.GetRSAPrivateKey()!.ExportRSAPrivateKeyPem());

    Dictionary<string, string?> baseConfig = new()
                                             {
                                               {
                                                 "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage"
                                               },
                                               {
                                                 "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage"
                                               },
                                               {
                                                 "Components:LeaseProvider", "ArmoniK.Adapters.MongoDB.LeaseProvider"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Hosts)}:0", "localhost:3232"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Tls)}", "true"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.User)}", "user"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.Password)}", "password"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.CredentialsPath)}", ""
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.CAFile)}", ""
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ReplicaSet)}", "rs0"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", "database"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DataRetention)}", "10.00:00:00"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.MaxConnectionPoolSize)}", "100"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelayMin", "00:00:10"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelayMax", "00:00:20"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ObjectStorage)}:ChunkSize", "100000"
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ClientCertificateFiles)}:0", path0
                                               },
                                               {
                                                 $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ClientCertificateFiles)}:1", path1
                                               },
                                             };

    var logger = NullLogger.Instance;

    configuration_ = new ConfigurationManager();
    configuration_.AddInMemoryCollection(baseConfig);

    var services = new ServiceCollection();
    services.AddMongoComponents(configuration_,
                                logger);
    services.AddSingleton(ActivitySource);
    services.AddLogging();
    provider_ = services.BuildServiceProvider(new ServiceProviderOptions
                                              {
                                                ValidateOnBuild = true,
                                              });
  }

  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");

  private ServiceProvider?      provider_;
  private ConfigurationManager? configuration_;

  [Test]
  public void MongoDbOptionsNotNull()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();
    Assert.NotNull(options);
  }

  [Test]
  public void MongoDbOptionsValueNotNull()
  {
    var options = configuration_!.GetRequiredValue<Options.MongoDB>(Options.MongoDB.SettingSection);
    Assert.NotNull(options);
  }

  [Test]
  public void ReadMongoDbHost()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("localhost:3232",
                    options.Hosts.Single());
  }

  [Test]
  public void ReadClientCertificateFiles()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(2,
                    options.ClientCertificateFiles.Count);
  }

  [Test]
  public void ReadMongoDbUser()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("user",
                    options.User);
  }

  [Test]
  public void ReadMongoDbPassword()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("password",
                    options.Password);
  }

  [Test]
  public void ReadMongoDbCredentialsPath()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("",
                    options.CredentialsPath);
  }

  [Test]
  public void ReadMongoDbCaFile()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual("",
                    options.CAFile);
  }

  [Test]
  public void ReadMongoDbTls()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(true,
                    options.Tls);
  }

  [Test]
  public void ReadMongoDbDatabaseName()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();
    Assert.AreEqual("database",
                    options.DatabaseName);
  }

  [Test]
  public void ReadMongoDbDataRetention()
  {
    var options = provider_!.GetRequiredService<Options.MongoDB>();

    Assert.AreEqual(TimeSpan.FromDays(10),
                    options.DataRetention);
  }

  [Test]
  public void TableOptionsNotNull()
  {
    var options = provider_!.GetRequiredService<TableStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadTablePollingMinDelay()
  {
    var options = provider_!.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(10),
                    options.PollingDelayMin);
  }

  [Test]
  public void ReadTablePollingMaxDelay()
  {
    var options = provider_!.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(20),
                    options.PollingDelayMax);
  }

  [Test]
  public void ObjectOptionsNotNull()
  {
    var options = provider_!.GetRequiredService<Options.ObjectStorage>();

    Assert.NotNull(options);
  }

  [Test]
  public void ReadObjectChunkSize()
  {
    var options = provider_!.GetRequiredService<Options.ObjectStorage>();

    Assert.AreEqual(100000,
                    options.ChunkSize);
  }

  [Test]
  public void BuildTableStorage()
  {
    var table = provider_!.GetRequiredService<TableStorage>();

    Assert.NotNull(table);
  }

  [Test]
  public void TableStorageHasPollingDelayMin()
  {
    var table = provider_!.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(10),
                    table.PollingDelayMin);
  }

  [Test]
  public void TableStorageHasPollingDelayMax()
  {
    var table = provider_!.GetRequiredService<TableStorage>();

    Assert.AreEqual(TimeSpan.FromSeconds(20),
                    table.PollingDelayMax);
  }

  [Test]
  public void BuildObjectStorage()
  {
    var objectStorage = provider_!.GetRequiredService<ObjectStorage>();

    Assert.NotNull(objectStorage);
  }

  [Test]
  public void ObjectStorageFactoryHasBindingToObjectStorage()
  {
    var objectStorage = provider_!.GetRequiredService<IObjectStorage>();

    Assert.NotNull(objectStorage);
    Assert.AreEqual(typeof(ObjectStorage),
                    objectStorage.GetType());
  }
}
