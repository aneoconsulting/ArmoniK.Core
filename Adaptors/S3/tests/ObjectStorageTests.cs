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

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Tests.TestBase;
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

  private static readonly string SolutionRoot =
    Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(typeof(ObjectStorageTests)
                                                                                                                                                .Assembly
                                                                                                                                                .Location))))) ??
                                  string.Empty));

  private static readonly string S3Path =
    $"{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net8.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.S3.dll";


  protected override void GetObjectStorageInstance()
  {
    Dictionary<string, string?> minimalConfig = new()
                                                {
                                                  {
                                                    "Components:ObjectStorageAdaptorSettings:ClassName", "ArmoniK.Core.Adapters.S3.ObjectBuilder"
                                                  },
                                                  {
                                                    "Components:ObjectStorageAdaptorSettings:AdapterAbsolutePath", $"{SolutionRoot}{S3Path}"
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

    services.AddAdapter(configuration,
                        nameof(Components.ObjectStorageAdaptorSettings),
                        logger.GetLogger());

    var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                 {
                                                   ValidateOnBuild = true,
                                                 });

    ObjectStorage = provider.GetRequiredService<IObjectStorage>();
    RunTests      = true;
  }
}
