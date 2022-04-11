// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Collections.Generic;
using System.Diagnostics;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using Mongo2Go;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
public class ObjectStorageTests : ObjectStorageTestBase
{
  public override void TearDown()
  {
    ObjectStorage = null;
    RunTests      = false;
  }

  private                 MongoDbRunner  runner_;
  private                 IMongoClient   client_;
  private const           string         DatabaseName   = "ArmoniK_TestDB";
  private static readonly ActivitySource ActivitySource = new("ArmoniK.Core.Adapters.MongoDB.Tests");

  public override void GetObjectStorageInstance()
  {
    var logger = NullLogger.Instance;
    runner_ = MongoDbRunner.Start(singleNodeReplSet: false,
                                  logger: logger);
    client_ = new MongoClient(runner_.ConnectionString);

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
                                               {
                                                 {
                                                   "Components:ObjectStorage", "ArmoniK.Adapters.MongoDB.ObjectStorage"
                                                 },
                                                 {
                                                   $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", DatabaseName
                                                 },
                                                 {
                                                   $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.ObjectStorage)}:PollingDelay", "00:00:10"
                                                 },
                                               };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddMongoStorages(configuration,
                              logger);
    services.AddSingleton(ActivitySource);
    services.AddTransient(serviceProvider => client_);
    services.AddLogging();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    services.AddSingleton<IObjectStorageFactory, ObjectStorageFactory>();

    var objectStorageFactory = provider.GetRequiredService<IObjectStorageFactory>();
    ObjectStorage = objectStorageFactory.CreateObjectStorage("storage");
    RunTests      = true;
  }
}
