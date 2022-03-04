// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
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


using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using Mongo2Go;

using MongoDB.Bson;
using MongoDB.Driver;


using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
public class ResultTableTests : ResultTableTestBase
{
  private MongoClient   client_;
  //private MongoDbRunner runner_;

  public override void GetResultTableInstance()
  {

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
    {
      { "Components:TableStorage", "ArmoniK.Adapters.MongoDB.TableStorage" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.DatabaseName)}", "ArmoniK_TestDB" },
      { $"{Options.MongoDB.SettingSection}:{nameof(Options.MongoDB.TableStorage)}:PollingDelay", "00:00:10" },
    };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    var logger   = NullLogger.Instance;
    services.AddMongoStorages(configuration, logger);

    //runner_ = MongoDbRunner.Start();
    //client_ = new MongoClient(runner_.ConnectionString);
    client_ = new MongoClient("mongodb://127.0.0.1:27017");
    services.AddTransient<IMongoClient>(serviceProvider => client_);

    services.AddLogging();
    var provider = services.BuildServiceProvider(new ServiceProviderOptions
    {
      ValidateOnBuild = true,
    });

    ResultTable = provider.GetRequiredService<IResultTable>();
    RunTests    = true;
  }

  public override void TearDown()
  {
    var db = client_.GetDatabase("ArmoniK_TestDB")
                    .GetCollection<BsonDocument>("Result");
    db.DeleteManyAsync(Builders<BsonDocument>.Filter.Empty);
    RunTests    = false;
  }
}