// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.TestBase;
using ArmoniK.Core.Utils;

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
    redis_?.Dispose();
    redis_        = null;
    ObjectStorage = null;
    RunTests      = false;
  }

  private RedisInside.Redis? redis_;

  public override void GetObjectStorageInstance()
  {
    redis_ = new RedisInside.Redis(configuration => configuration.Port(Random.Shared.Next(1000,
                                                                                          2000)));

    // Minimal set of configurations to operate on a toy DB
    Dictionary<string, string> minimalConfig = new()
                                               {
                                                 {
                                                   "Components:ObjectStorage", "ArmoniK.Adapters.Redis.ObjectStorage"
                                                 },
                                                 {
                                                   "Redis:MaxRetry", "5"
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

    services.AddSingleton<IDatabaseAsync>(_ => ConnectionMultiplexer.Connect(config,
                                                                             TextWriter.Null)
                                                                    .GetDatabase());
    services.AddSingleton<IObjectStorageFactory, ObjectStorageFactory>();

    services.AddOption(configuration,
                       Options.Redis.SettingSection,
                       out Options.Redis redisOptions);

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    ObjectStorageFactory = provider.GetRequiredService<IObjectStorageFactory>();
    ObjectStorage        = ObjectStorageFactory.CreateObjectStorage("storage");
    RunTests             = true;
  }
}
