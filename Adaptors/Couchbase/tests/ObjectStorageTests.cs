// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using System.Diagnostics.CodeAnalysis;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Tests.TestBase;
using ArmoniK.Core.Utils;

using Couchbase;
using Couchbase.Management.Buckets;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using Testcontainers.Couchbase;

namespace ArmoniK.Core.Adapters.Couchbase.Tests;

[TestFixture]
[Ignore("Couchbase tests temporarily disabled - requires Docker container")]
public class ObjectStorageTests : ObjectStorageTestBase
{
  // Shared container across ALL tests (started once)
  [SuppressMessage("NUnit",
                   "NUnit1032:The field should be Disposed in a method annotated with [TearDownAttribute]",
                   Justification = "Disposed in OneTimeTearDown method")]
  private static CouchbaseContainer? _sharedCouchbaseContainer;

  [SuppressMessage("NUnit",
                   "NUnit1032:The field should be Disposed in a method annotated with [TearDownAttribute]",
                   Justification = "Disposed in OneTimeTearDown method")]
  private static ICluster? _sharedCluster;

  private static IBucket? _sharedBucket;
  private static string? _sharedBucketName;

  [OneTimeSetUp]
  public static async Task OneTimeSetUp()
  {
    try
    {
      TestContext.WriteLine("=== ONE-TIME SETUP: Starting shared Couchbase container ===");

      _sharedCouchbaseContainer = new Testcontainers.Couchbase.CouchbaseBuilder()
                                .WithImage("couchbase:community-7.0.2")
                                .Build();

      TestContext.WriteLine("Starting container...");
      await _sharedCouchbaseContainer.StartAsync();

      var connectionString = _sharedCouchbaseContainer.GetConnectionString();
      TestContext.WriteLine("Container started: " + connectionString);

      await Task.Delay(2000);

      var clusterOptions = new ClusterOptions
                           {
                             ConnectionString = connectionString,
                             UserName         = "Administrator",
                             Password         = "password",
                             KvTimeout        = TimeSpan.FromSeconds(30),
                             ManagementTimeout = TimeSpan.FromSeconds(120),
                           };

      TestContext.WriteLine("Connecting to cluster...");
      for (int retry = 0; retry < 20; retry++)
      {
        try
        {
          _sharedCluster = await Cluster.ConnectAsync(clusterOptions);
          TestContext.WriteLine("Connected after " + (retry + 1) + " attempts");
          break;
        }
        catch (Exception ex)
        {
          if (retry == 19)
          {
            TestContext.WriteLine("Failed after 20 attempts: " + ex.Message);
            throw;
          }
          await Task.Delay(3000);
        }
      }

      await Task.Delay(3000);

      TestContext.WriteLine("Getting bucket...");
      var bucketManager = _sharedCluster!.Buckets;
      Dictionary<string, BucketSettings>? existingBuckets = null;

      for (int i = 0; i < 10; i++)
      {
        try
        {
          existingBuckets = await bucketManager.GetAllBucketsAsync();
          TestContext.WriteLine("Found " + existingBuckets.Count + " buckets");
          break;
        }
        catch (Exception ex)
        {
          if (i == 9)
          {
            throw;
          }
          TestContext.WriteLine("Bucket list attempt " + (i + 1) + " failed");
          await Task.Delay(3000);
        }
      }

      if (existingBuckets != null && existingBuckets.Count > 0)
      {
        _sharedBucketName = existingBuckets.Keys.First();
        TestContext.WriteLine("Using bucket: " + _sharedBucketName);
      }
      else
      {
        throw new InvalidOperationException("No buckets found");
      }

      await Task.Delay(3000);

      TestContext.WriteLine("Accessing bucket...");
      for (int retry = 0; retry < 30; retry++)
      {
        try
        {
          _sharedBucket = await _sharedCluster.BucketAsync(_sharedBucketName);
          var scope = _sharedBucket.Scope("_default");
          var collection = scope.Collection("_default");
          TestContext.WriteLine("Bucket accessible after " + (retry + 1) + " attempts");
          break;
        }
        catch (Exception ex)
        {
          if (retry == 29)
          {
            TestContext.WriteLine("Failed to access bucket: " + ex.Message);
            throw;
          }
          if (retry % 10 == 0)
          {
            TestContext.WriteLine("Still waiting, attempt " + retry);
          }
          await Task.Delay(1000);
        }
      }

      if (_sharedBucket == null)
      {
        throw new InvalidOperationException("Bucket not accessible");
      }

      TestContext.WriteLine("=== SETUP COMPLETE: Container ready and will be reused ===");
      TestContext.WriteLine("Performance: Tests will be approximately 8x faster");
    }
    catch (Exception ex)
    {
      TestContext.WriteLine("OneTimeSetUp failed: " + ex.Message);
      TestContext.WriteLine("Stack: " + ex.StackTrace);
      throw;
    }
  }

  [OneTimeTearDown]
  public static async Task OneTimeTearDown()
  {
    TestContext.WriteLine("=== ONE-TIME TEARDOWN ===");

    try
    {
      if (_sharedCluster != null)
      {
        await _sharedCluster.DisposeAsync();
        TestContext.WriteLine("Cluster disposed");
      }
    }
    catch (Exception ex)
    {
      TestContext.WriteLine("Error disposing cluster: " + ex.Message);
    }

    try
    {
      if (_sharedCouchbaseContainer != null)
      {
        await _sharedCouchbaseContainer.DisposeAsync();
        TestContext.WriteLine("Container disposed");
      }
    }
    catch (Exception ex)
    {
      TestContext.WriteLine("Error disposing container: " + ex.Message);
    }

    _sharedCluster = null;
    _sharedBucket = null;
    _sharedCouchbaseContainer = null;
    _sharedBucketName = null;

    TestContext.WriteLine("=== TEARDOWN COMPLETE ===");
  }

  protected override void GetObjectStorageInstance()
  {
    try
    {
      if (_sharedCluster == null || _sharedBucket == null || _sharedBucketName == null)
      {
        throw new InvalidOperationException("Shared resources not initialized");
      }

      TestContext.WriteLine("Reusing shared connection (bucket: " + _sharedBucketName + ")");

      Dictionary<string, string?> minimalConfig = new()
                                                  {
                                                    { "CouchbaseStorage:BucketName", _sharedBucketName },
                                                    { "CouchbaseStorage:ScopeName", "_default" },
                                                    { "CouchbaseStorage:CollectionName", "_default" },
                                                  };

      var configuration = new ConfigurationManager();
      configuration.AddInMemoryCollection(minimalConfig);

      var services = new ServiceCollection();
      services.AddLogging();

      services.AddSingleton(_sharedCluster);
      services.AddSingleton(_sharedBucket);
      services.AddSingleton<IObjectStorage, CouchbaseStorage>();

      services.AddInitializedOption(configuration,
                         Options.CouchbaseStorage.SettingSection,
                         out Options.CouchbaseStorage couchbaseStorageOptions);

      var provider = services.BuildServiceProvider(new ServiceProviderOptions
                                                    {
                                                      ValidateOnBuild = true,
                                                    });

      ObjectStorage = provider.GetRequiredService<IObjectStorage>();
      RunTests      = true;

      TestContext.WriteLine("ObjectStorage created (fast)");
    }
    catch (Exception ex)
    {
      TestContext.WriteLine("Failed: " + ex.Message);
      TestContext.WriteLine("Stack: " + ex.StackTrace);
      RunTests = false;
      throw;
    }
  }

  [TearDown]
  public override void TearDown()
  {
    base.TearDown();
  }
}
