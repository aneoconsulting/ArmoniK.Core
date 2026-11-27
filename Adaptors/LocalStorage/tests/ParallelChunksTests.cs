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
using System.Threading.Tasks;

using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Testing;
using ArmoniK.Core.Utils.Uuid;

using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.LocalStorage.Tests;

/// <summary>
///   The tests shared by all the object storages, with 4 chunks of 8 bytes read and written concurrently
/// </summary>
[TestFixture]
public class ParallelChunksObjectStorageTests : ObjectStorageTestBase
{
  public override void TearDown()
  {
    ObjectStorage = null;
    RunTests      = false;
  }

  protected override void GetObjectStorageInstance()
  {
    var rootPath = Path.Combine(Path.GetTempPath(),
                                $"ArmoniK.Parallel.{Environment.ProcessId}");

    if (Directory.Exists(rootPath))
    {
      Directory.Delete(rootPath,
                       true);
    }

    ObjectStorage = new ObjectStorage(new Options.LocalStorage
                                      {
                                        ChunkSize        = 8,
                                        ChunkParallelism = 4,
                                        Path             = rootPath,
                                      },
                                      new UuidGeneratorV4(),
                                      NullLogger<ObjectStorage>.Instance);
    RunTests = true;
  }
}

[TestFixture]
public class ParallelChunksTests
{
  [SetUp]
  public void SetUp()
  {
    rootPath_ = Path.Combine(Path.GetTempPath(),
                             $"ArmoniK.Chunks.{Environment.ProcessId}.{Guid.NewGuid()}");
    Directory.CreateDirectory(rootPath_);
  }

  [TearDown]
  public void TearDown()
    => Directory.Delete(rootPath_,
                        true);

  private string rootPath_ = "";

  private ObjectStorage CreateStorage(int chunkSize,
                                      int chunkParallelism)
    => new(new Options.LocalStorage
           {
             ChunkSize        = chunkSize,
             ChunkParallelism = chunkParallelism,
             Path             = rootPath_,
             SplitPathAt      = 0,
           },
           new UuidGeneratorV4(),
           NullLogger<ObjectStorage>.Instance);

  private static byte[] RandomBytes(int length)
  {
    var bytes = new byte[length];
    new Random(length).NextBytes(bytes);
    return bytes;
  }

  // Cuts the data in input chunks of the given size, unrelated to ChunkSize
  private static async IAsyncEnumerable<ReadOnlyMemory<byte>> Chunks(byte[] data,
                                                                     int    inputChunkSize)
  {
    for (var offset = 0; offset < data.Length; offset += inputChunkSize)
    {
      await Task.Yield();
      yield return data.AsMemory(offset,
                                 Math.Min(inputChunkSize,
                                          data.Length - offset));
    }
  }

  private static ObjectData Data
    => new()
       {
         ResultId  = "ResultId",
         SessionId = "SessionId",
       };

  public static IEnumerable<TestCaseData> Cases()
  {
    foreach (var parallelism in new[]
                                {
                                  1,
                                  2,
                                  4,
                                  0,
                                })
    {
      foreach (var chunkSize in new[]
                                {
                                  1,
                                  5,
                                  16,
                                })
      {
        // empty, smaller than a chunk, exact multiples, and in between; input chunks smaller and larger than ChunkSize
        foreach (var length in new[]
                               {
                                 0,
                                 1,
                                 chunkSize,
                                 chunkSize + 1,
                                 4 * chunkSize,
                                 100,
                               }.Distinct())
        {
          foreach (var inputChunkSize in new[]
                                         {
                                           3,
                                           37,
                                         })
          {
            yield return new TestCaseData(parallelism,
                                          chunkSize,
                                          length,
                                          inputChunkSize);
          }
        }
      }
    }
  }

  [TestCaseSource(nameof(Cases))]
  public async Task RoundTripShouldKeepTheData(int parallelism,
                                               int chunkSize,
                                               int length,
                                               int inputChunkSize)
  {
    var storage = CreateStorage(chunkSize,
                                parallelism);
    var data = RandomBytes(length);

    var (id, size) = await storage.AddOrUpdateAsync(Data,
                                                    Chunks(data,
                                                           inputChunkSize));
    var chunks = await storage.GetValuesAsync(id)
                              .ToListAsync();

    Assert.Multiple(() =>
                    {
                      Assert.That(size,
                                  Is.EqualTo(length));
                      Assert.That(chunks.SelectMany(chunk => chunk),
                                  Is.EqualTo(data));
                      // every chunk but the last one is full
                      Assert.That(chunks.SkipLast(1)
                                        .All(chunk => chunk.Length == chunkSize),
                                  Is.True);
                      Assert.That(chunks.All(chunk => chunk.Length is > 0 && chunk.Length <= chunkSize),
                                  Is.True);
                    });
  }

  [Test]
  public void FailedWriteShouldRemoveTheFile()
  {
    var storage = CreateStorage(4,
                                4);

    async IAsyncEnumerable<ReadOnlyMemory<byte>> Failing()
    {
      yield return RandomBytes(30);
      await Task.Yield();
      throw new InvalidOperationException("producer failure");
    }

    Assert.ThrowsAsync<InvalidOperationException>(() => storage.AddOrUpdateAsync(Data,
                                                                                 Failing()));
    Assert.That(Directory.GetFiles(rootPath_),
                Is.Empty);
  }

  [Test]
  public async Task StoppingTheReadEarlyShouldReleaseTheFile()
  {
    var storage = CreateStorage(4,
                                4);
    var (id, _) = await storage.AddOrUpdateAsync(Data,
                                                 Chunks(RandomBytes(1000),
                                                        64));

    await foreach (var _ in storage.GetValuesAsync(id))
    {
      break;
    }

    await storage.TryDeleteAsync([id]);
    Assert.That(Directory.GetFiles(rootPath_),
                Is.Empty);
  }
}
