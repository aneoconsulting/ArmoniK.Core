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
using System.IO;

using ArmoniK.Core.Adapters.PostgresSQL.Options;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgresSQL.Tests;

[TestFixture]
internal class InjectionTests
{
  [SetUp]
  public void SetUp()
  {
    credentialsFile_ = Path.GetTempFileName();
    File.WriteAllText(credentialsFile_,
                      """
                      {
                        "PostgreSQL": {
                          "Password": "password-from-credentials-file"
                        }
                      }
                      """);

    Dictionary<string, string?> baseConfig = new()
                                             {
                                               {
                                                 $"{PostgreSQL.SettingSection}:{nameof(PostgreSQL.Host)}", "localhost"
                                               },
                                               {
                                                 $"{PostgreSQL.SettingSection}:{nameof(PostgreSQL.User)}", "user"
                                               },
                                               {
                                                 $"{PostgreSQL.SettingSection}:{nameof(PostgreSQL.Password)}", "password-from-base-config"
                                               },
                                               {
                                                 $"{PostgreSQL.SettingSection}:{nameof(PostgreSQL.CredentialsPath)}", credentialsFile_
                                               },
                                             };

    var logger = NullLogger.Instance;

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(baseConfig);

    var services = new ServiceCollection();
    services.AddPostgresClient(configuration,
                               logger);
    provider_ = services.BuildServiceProvider();
  }

  [TearDown]
  public void TearDown()
  {
    provider_?.Dispose();

    if (credentialsFile_ is not null && File.Exists(credentialsFile_))
    {
      File.Delete(credentialsFile_);
    }
  }

  private string?          credentialsFile_;
  private ServiceProvider? provider_;

  [Test]
  public void PostgresCredentialsPathShouldOverridePassword()
  {
    var options = provider_!.GetRequiredService<PostgreSQL>();

    Assert.That(options.Password,
                Is.EqualTo("password-from-credentials-file"));
  }
}
