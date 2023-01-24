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

using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.S3.Tests;

[TestFixture]
public class ObjectStorageTests : ObjectStorageTestBase
{
  public override void TearDown()
  {
    ObjectStorage = null;
    RunTests      = false;
  }

  public override void GetObjectStorageInstance()
  {
    Dictionary<string, string> minimalConfig = new()
                                               {
                                                 {
                                                   "Components:ObjectStorage", "ArmoniK.Adapters.S3.ObjectStorage"
                                                 },
                                                 {
                                                   "S3:BucketName", "miniobucket"
                                                 },
                                                 {
                                                   "S3:EndpointUrl", "http://127.0.0.1:9000"
                                                 },
                                                 {
                                                   "S3:Login", "minioadmin"
                                                 },
                                                 {
                                                   "S3:Password", "minioadmin"
                                                 },
                                                 {
                                                   "S3:MustForcePathStyle", "true"
                                                 },
                                               };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var services = new ServiceCollection();
    services.AddLogging();
    var logger = new LoggerInit(configuration);

    services.AddS3(configuration,
                   logger.GetLogger());

    services.AddSingleton<IObjectStorageFactory, ObjectStorageFactory>();

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    ObjectStorageFactory = provider.GetRequiredService<IObjectStorageFactory>();
    ObjectStorage        = ObjectStorageFactory.CreateObjectStorage("storage");
    RunTests             = true;
  }
}
