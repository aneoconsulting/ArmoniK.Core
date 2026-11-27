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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;
using ArmoniK.Core.Common.Testing;
using ArmoniK.Core.Utils.Uuid;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.LocalStorage.Tests;

/// <summary>
///   The tests shared by all the object storages, on objects striped over 3 files with stripes smaller than the chunks
/// </summary>
[TestFixture]
public class StripedObjectStorageTests : ObjectStorageTestBase
{
  public override void TearDown()
  {
    ObjectStorage = null;
    RunTests      = false;
  }

  protected override void GetObjectStorageInstance()
  {
    var rootPath = Path.Combine(Path.GetTempPath(),
                                $"ArmoniK.Striped.{Environment.ProcessId}");

    if (Directory.Exists(rootPath))
    {
      Directory.Delete(rootPath,
                       true);
    }

    ObjectStorage = new ObjectStorage(new Options.LocalStorage
                                      {
                                        ChunkSize   = 8,
                                        Path        = rootPath,
                                        StripeCount = 3,
                                        StripeSize  = 5,
                                      },
                                      new UuidGeneratorV4(),
                                      NullLogger<ObjectStorage>.Instance);
    RunTests = true;
  }
}

[TestFixture]
public class StripedLayoutTests
{
  [SetUp]
  public void SetUp()
  {
    rootPath_ = Path.Combine(Path.GetTempPath(),
                             $"ArmoniK.Layout.{Environment.ProcessId}.{Guid.NewGuid()}");
    Directory.CreateDirectory(rootPath_);
  }

  [TearDown]
  public void TearDown()
    => Directory.Delete(rootPath_,
                        true);

  private string rootPath_ = "";

  private ObjectStorage CreateStorage(int stripeCount,
                                      int stripeSize,
                                      int chunkSize = 16)
    => new(new Options.LocalStorage
           {
             ChunkSize   = chunkSize,
             Path        = rootPath_,
             StripeCount = stripeCount,
             StripeSize  = stripeSize,
             SplitPathAt = 0,
           },
           new UuidGeneratorV4(),
           NullLogger<ObjectStorage>.Instance);

  private static byte[] RandomBytes(int length)
  {
    var bytes = new byte[length];
    new Random(length).NextBytes(bytes);
    return bytes;
  }

  // Cuts the data in input chunks of the given size, unrelated to the stripes and the read chunks
  private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(byte[] data,
                                                                     int    inputChunkSize)
  {
    for (var offset = 0; offset < data.Length; offset += inputChunkSize)
    {
      yield return data.AsMemory(offset,
                                 Math.Min(inputChunkSize,
                                          data.Length - offset));
    }

    await Task.CompletedTask;
  }

  private static Task<(byte[] id, long size)> Write(ObjectStorage storage,
                                                    byte[]        data,
                                                    int           inputChunkSize = 7)
    => storage.AddOrUpdateAsync(new ObjectData
                                {
                                  ResultId  = "ResultId",
                                  SessionId = "SessionId",
                                },
                                Chunks(data,
                                       inputChunkSize));

  private static async Task<List<byte[]>> Read(ObjectStorage storage,
                                               byte[]        id)
  {
    var chunks = new List<byte[]>();
    await foreach (var chunk in storage.GetValuesAsync(id))
    {
      chunks.Add(chunk);
    }

    return chunks;
  }

  public static IEnumerable<TestCaseData> Layouts()
  {
    foreach (var stripeCount in new[]
                                {
                                  1,
                                  2,
                                  3,
                                  4,
                                })
    {
      foreach (var stripeSize in new[]
                                 {
                                   1,
                                   5,
                                   16,
                                 })
      {
        // empty, smaller than a stripe, exact multiples of the stripe size and of a full round, and in between
        foreach (var length in new[]
                               {
                                 0,
                                 1,
                                 stripeSize,
                                 stripeSize + 1,
                                 stripeCount * stripeSize,
                                 stripeCount * stripeSize + 1,
                                 100,
                               }.Distinct())
        {
          yield return new TestCaseData(stripeCount,
                                        stripeSize,
                                        length);
        }
      }
    }
  }

  [TestCaseSource(nameof(Layouts))]
  public async Task RoundTripShouldKeepTheData(int stripeCount,
                                               int stripeSize,
                                               int length)
  {
    var storage = CreateStorage(stripeCount,
                                stripeSize,
                                3);
    var data = RandomBytes(length);

    var (id, size) = await Write(storage,
                                 data);
    var chunks = await Read(storage,
                            id);
    var sizes = await storage.GetSizesAsync([id]);

    Assert.Multiple(() =>
                    {
                      Assert.That(size,
                                  Is.EqualTo(length));
                      Assert.That(chunks.SelectMany(chunk => chunk),
                                  Is.EqualTo(data));
                      Assert.That(chunks.All(chunk => chunk.Length is > 0 and <= 3),
                                  Is.True);
                      Assert.That(sizes.Single()
                                       .Value,
                                  Is.EqualTo(length));
                      Assert.That(Directory.GetFiles(rootPath_),
                                  Has.Length.EqualTo(stripeCount));
                    });
  }

  [Test]
  public async Task StripesShouldBeDealtRoundRobin()
  {
    var storage = CreateStorage(3,
                                2);
    var (id, _) = await Write(storage,
                              Encoding.ASCII.GetBytes("aabbccddeef"));

    var key = Encoding.UTF8.GetString(id);
    Assert.Multiple(() =>
                    {
                      Assert.That(key,
                                  Does.EndWith("_3x2"));
                      Assert.That(File.ReadAllText(Path.Combine(rootPath_,
                                                                $"{key}.0")),
                                  Is.EqualTo("aadd"));
                      Assert.That(File.ReadAllText(Path.Combine(rootPath_,
                                                                $"{key}.1")),
                                  Is.EqualTo("bbee"));
                      Assert.That(File.ReadAllText(Path.Combine(rootPath_,
                                                                $"{key}.2")),
                                  Is.EqualTo("ccf"));
                    });
  }

  [Test]
  public async Task ObjectsShouldBeReadWhateverTheSettingsOfTheReader()
  {
    var data = RandomBytes(1000);
    var (stripedId, _) = await Write(CreateStorage(4,
                                                   10),
                                     data);
    var (plainId, _) = await Write(CreateStorage(1,
                                                 10),
                                   data);

    foreach (var reader in new[]
                           {
                             CreateStorage(1,
                                           10),
                             CreateStorage(2,
                                           64),
                           })
    {
      Assert.That((await Read(reader,
                              stripedId)).SelectMany(chunk => chunk),
                  Is.EqualTo(data));
      Assert.That((await Read(reader,
                              plainId)).SelectMany(chunk => chunk),
                  Is.EqualTo(data));
    }
  }

  [Test]
  public async Task DeleteShouldRemoveAllTheFiles()
  {
    var storage = CreateStorage(3,
                                4);
    var (id, _) = await Write(storage,
                              RandomBytes(50));

    await storage.TryDeleteAsync([id]);

    Assert.Multiple(async () =>
                    {
                      Assert.That(Directory.GetFiles(rootPath_),
                                  Is.Empty);
                      Assert.That((await storage.GetSizesAsync([id])).Single()
                                                                     .Value,
                                  Is.Null);
                    });
  }

  [Test]
  public async Task MissingFileShouldBeNotFound()
  {
    var storage = CreateStorage(3,
                                4);
    var (id, _) = await Write(storage,
                              RandomBytes(50));
    File.Delete(Path.Combine(rootPath_,
                             $"{Encoding.UTF8.GetString(id)}.1"));

    Assert.ThrowsAsync<ObjectDataNotFoundException>(() => Read(storage,
                                                               id));
    Assert.That((await storage.GetSizesAsync([id])).Single()
                                                   .Value,
                Is.Null);
  }

  [Test]
  public async Task StoppingTheReadEarlyShouldReleaseTheFiles()
  {
    var storage = CreateStorage(4,
                                8,
                                4);
    var (id, _) = await Write(storage,
                              RandomBytes(1000));

    await foreach (var _ in storage.GetValuesAsync(id))
    {
      break;
    }

    await storage.TryDeleteAsync([id]);
    Assert.That(Directory.GetFiles(rootPath_),
                Is.Empty);
  }

  [Test]
  public void FailedWriteShouldRemoveAllTheFiles()
  {
    var storage = CreateStorage(3,
                                4);

    async IAsyncEnumerable<ReadOnlyMemory<byte>> Failing()
    {
      yield return RandomBytes(30);
      await Task.Yield();
      throw new InvalidOperationException("producer failure");
    }

    Assert.ThrowsAsync<InvalidOperationException>(() => storage.AddOrUpdateAsync(new ObjectData
                                                                                 {
                                                                                   ResultId  = "ResultId",
                                                                                   SessionId = "SessionId",
                                                                                 },
                                                                                 Failing()));
    Assert.That(Directory.GetFiles(rootPath_),
                Is.Empty);
  }
}
