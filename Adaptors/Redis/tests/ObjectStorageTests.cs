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
using System.IO;

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis.Tests;

[TestFixture]
public class ObjectStorageTests : ObjectStorageTestBase
{
  public override void TearDown()
  {
    redis_.Dispose();
    ObjectStorage = null;
    RunTests      = false;
  }

  private RedisInside.Redis redis_;

  public override void GetObjectStorageInstance()
  {
    redis_ = new RedisInside.Redis();

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
                                               {
                                                 {
                                                   "Components:ObjectStorage", "ArmoniK.Adapters.Redis.ObjectStorage"
                                                 },
                                               };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddLogging();

    var config = new ConfigurationOptions
                 {
                   ReconnectRetryPolicy = new ExponentialRetry(10),
                   AbortOnConnectFail   = true,
                   EndPoints =
                   {
                     redis_.Endpoint,
                   },
                 };

    services.AddSingleton<IConnectionMultiplexer>(_ =>
                                                  {
                                                    var multiplexer = ConnectionMultiplexer.Connect(config,
                                                                                                    TextWriter.Null);

                                                    multiplexer.IncludeDetailInExceptions              = true;
                                                    multiplexer.IncludePerformanceCountersInExceptions = true;
                                                    return multiplexer;
                                                  });

    services.AddSingleton<IObjectStorageFactory, ObjectStorageFactory>();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    var objectStorageFactory = provider.GetRequiredService<IObjectStorageFactory>();
    ObjectStorage = objectStorageFactory.CreateObjectStorage("storage");
    RunTests      = true;
  }
}
