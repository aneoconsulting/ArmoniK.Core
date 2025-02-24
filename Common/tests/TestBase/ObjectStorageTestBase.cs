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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Core.Base;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Base.Exceptions;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class ObjectStorageTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetObjectStorageInstance();

    if (!RunTests || CheckForSkipSetup())
    {
      return;
    }

    await ObjectStorage!.Init(CancellationToken.None)
                        .ConfigureAwait(false);

    var dataBytesList = new List<ReadOnlyMemory<byte>>
                        {
                          Encoding.ASCII.GetBytes("AAAA"),
                          Encoding.ASCII.GetBytes("BBBB"),
                          Encoding.ASCII.GetBytes("CCCC"),
                          Encoding.ASCII.GetBytes("DDDD"),
                        };
    datakey1_ = (await ObjectStorage!.AddOrUpdateAsync(new ObjectData
                                                       {
                                                         ResultId  = "ResultId",
                                                         SessionId = "SessionId",
                                                       },
                                                       dataBytesList.ToAsyncEnumerable())
                                     .ConfigureAwait(false)).id;

    dataBytesList = new List<ReadOnlyMemory<byte>>
                    {
                      Encoding.ASCII.GetBytes("AAAABBBB"),
                    };
    datakey2_ = (await ObjectStorage.AddOrUpdateAsync(new ObjectData
                                                      {
                                                        ResultId  = "ResultId",
                                                        SessionId = "SessionId",
                                                      },
                                                      dataBytesList.ToAsyncEnumerable())
                                    .ConfigureAwait(false)).id;

    dataBytesList = new List<ReadOnlyMemory<byte>>
                    {
                      Array.Empty<byte>(),
                    };
    datakeyEmpty_ = (await ObjectStorage.AddOrUpdateAsync(new ObjectData
                                                          {
                                                            ResultId  = "ResultId",
                                                            SessionId = "SessionId",
                                                          },
                                                          dataBytesList.ToAsyncEnumerable())
                                        .ConfigureAwait(false)).id;
  }

  [TearDown]
  public virtual void TearDown()
  {
    ObjectStorage = null;
    RunTests      = false;
  }

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
  }

  /* Interface to test */
  protected IObjectStorage? ObjectStorage;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool    RunTests;
  private   byte[]? datakey1_;
  private   byte[]? datakey2_;
  private   byte[]? datakeyEmpty_;

  /* Function be override so it returns the suitable instance
   * of TaskTable to the corresponding interface implementation */
  protected virtual void GetObjectStorageInstance()
  {
  }

  [Test]
  [Category("SkipSetUp")]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ObjectStorage!.Check(HealthCheckTag.Liveness)
                                              .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ObjectStorage.Check(HealthCheckTag.Readiness)
                                             .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ObjectStorage.Check(HealthCheckTag.Startup)
                                             .ConfigureAwait(false)).Status);

      await ObjectStorage.Init(CancellationToken.None)
                         .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await ObjectStorage.Check(HealthCheckTag.Liveness)
                                          .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ObjectStorage.Check(HealthCheckTag.Readiness)
                                          .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ObjectStorage.Check(HealthCheckTag.Startup)
                                          .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  public async Task AddValuesAsyncWithoutChunkShouldWork()
  {
    if (RunTests)
    {
      var (id, size) = await ObjectStorage!.AddOrUpdateAsync(new ObjectData
                                                             {
                                                               ResultId  = "ResultId",
                                                               SessionId = "SessionId",
                                                             },
                                                             AsyncEnumerable.Empty<ReadOnlyMemory<byte>>())
                                           .ConfigureAwait(false);
      var data = new List<byte>();
      await foreach (var chunk in ObjectStorage!.GetValuesAsync(id)
                                                .ConfigureAwait(false))
      {
        data.AddRange(chunk);
      }

      Assert.AreEqual(0,
                      data.Count);
      Assert.AreEqual(0,
                      size);
    }
  }

  [Test]
  [TestCase]
  [TestCase("")]
  [TestCase("abc")]
  [TestCase("",
            "")]
  [TestCase("abc",
            "")]
  [TestCase("abc",
            "def")]
  [TestCase("",
            "def")]
  [TestCase("abc",
            "def",
            "ghi")]
  public async Task AddValuesAsyncShouldWork(params string[] inputChunks)
  {
    if (RunTests)
    {
      var (id, size) = await ObjectStorage!.AddOrUpdateAsync(new ObjectData
                                                             {
                                                               ResultId  = "ResultId",
                                                               SessionId = "SessionId",
                                                             },
                                                             inputChunks.ToAsyncEnumerable()
                                                                        .Select(s => (ReadOnlyMemory<byte>)Encoding.ASCII.GetBytes(s)))
                                           .ConfigureAwait(false);

      var data = new List<byte>();
      await foreach (var chunk in ObjectStorage!.GetValuesAsync(id)
                                                .ConfigureAwait(false))
      {
        data.AddRange(chunk);
      }

      var input = Encoding.ASCII.GetBytes(string.Join(null,
                                                      inputChunks));

      Assert.AreEqual(input,
                      data);
      Assert.AreEqual(input.Length,
                      size);


      var dict = await ObjectStorage!.GetSizesAsync(new[]
                                                    {
                                                      id,
                                                    }!)
                                     .ConfigureAwait(false);

      Assert.That(dict.Keys.Count,
                  Is.EqualTo(1));
      Assert.That(dict[id],
                  Is.EqualTo(size));
    }
  }

  [Test]
  public async Task GetValuesAsyncShouldFail()
  {
    if (RunTests)
    {
      var id   = Encoding.UTF8.GetBytes("IdThatShouldFail");
      var data = new List<byte>();

      try
      {
        var res = ObjectStorage!.GetValuesAsync(id);


        await foreach (var chunk in res.ConfigureAwait(false))
        {
          data.AddRange(chunk);
        }

        Assert.AreEqual(id,
                        data.ToArray());
      }
      catch (ObjectDataNotFoundException)
      {
      }
      catch (Exception)
      {
        Assert.Fail("When value should not be available, it should either throw or return the value");
      }
    }
  }

  [Test]
  public async Task PayloadShouldBeEqual()
  {
    if (RunTests)
    {
      var res  = ObjectStorage!.GetValuesAsync(datakey2_!);
      var data = new List<byte>();
      foreach (var item in await res.ToListAsync()
                                    .ConfigureAwait(false))
      {
        data.AddRange(item);
      }

      var str = Encoding.ASCII.GetString(data.ToArray());
      Console.WriteLine(str);
      Assert.AreEqual("AAAABBBB",
                      str);
    }
  }

  [Test]
  public async Task Payload2ShouldBeEqual()
  {
    if (RunTests)
    {
      var res = ObjectStorage!.GetValuesAsync(datakey1_!);
      // var data = await res.AggregateAsync((bytes1, bytes2) => bytes1.Concat(bytes2).ToArray());
      var data = new List<byte>();
      foreach (var item in await res.ToListAsync()
                                    .ConfigureAwait(false))
      {
        data.AddRange(item);
      }

      var str = Encoding.ASCII.GetString(data.ToArray());
      Console.WriteLine(str);
      Assert.AreEqual("AAAABBBBCCCCDDDD",
                      str);
    }
  }

  [Test]
  public async Task EmptyPayload()
  {
    if (RunTests)
    {
      var res = await ObjectStorage!.GetValuesAsync(datakeyEmpty_!)
                                    .ToListAsync()
                                    .ConfigureAwait(false);
      Console.WriteLine(res.Count);
      var data = new List<byte>();
      foreach (var item in res)
      {
        data.AddRange(item);
      }

      var str = Encoding.ASCII.GetString(data.ToArray());
      Console.WriteLine(str);
      Assert.AreEqual(string.Empty,
                      str);

      var dict = await ObjectStorage!.GetSizesAsync(new[]
                                                    {
                                                      datakeyEmpty_!,
                                                    })
                                     .ConfigureAwait(false);
      Assert.AreEqual(1,
                      dict.Count);
      Assert.AreEqual(0,
                      dict[datakeyEmpty_!]);
    }
  }

  [Test]
  public async Task DeleteDeleteTwiceShouldSucceed()
  {
    if (RunTests)
    {
      var listChunks = new List<ReadOnlyMemory<byte>>
                       {
                         Encoding.ASCII.GetBytes("Armonik Payload chunk"),
                         Encoding.ASCII.GetBytes("Data 1"),
                         Encoding.ASCII.GetBytes("Data 2"),
                         Encoding.ASCII.GetBytes("Data 3"),
                         Encoding.ASCII.GetBytes("Data 4"),
                       };

      var (id, size) = await ObjectStorage!.AddOrUpdateAsync(new ObjectData
                                                             {
                                                               ResultId  = "ResultId",
                                                               SessionId = "SessionId",
                                                             },
                                                             listChunks.ToAsyncEnumerable())
                                           .ConfigureAwait(false);

      var dict = await ObjectStorage!.GetSizesAsync(new[]
                                                    {
                                                      id,
                                                    }!)
                                     .ConfigureAwait(false);

      Assert.That(dict.Keys.Count,
                  Is.EqualTo(1));
      Assert.That(dict[id],
                  Is.EqualTo(size));

      await ObjectStorage!.TryDeleteAsync(new[]
                                          {
                                            id,
                                          })
                          .ConfigureAwait(false);

      await ObjectStorage!.TryDeleteAsync(new[]
                                          {
                                            id,
                                          })
                          .ConfigureAwait(false);
    }
  }


  [Test]
  public async Task ExistsShouldSucceed()
  {
    if (RunTests)
    {
      var dict = await ObjectStorage!.GetSizesAsync(new[]
                                                    {
                                                      datakey1_,
                                                      datakey2_,
                                                    }!)
                                     .ConfigureAwait(false);

      Assert.That(dict.Keys.Count,
                  Is.EqualTo(2));
      Assert.That(dict[datakey1_!],
                  Is.GreaterThan(0));
      Assert.That(dict[datakey2_!],
                  Is.GreaterThan(0));
    }
  }

  [Test]
  public async Task ExistsWithBigDataShouldSucceed()
  {
    if (RunTests)
    {
      var channel = Channel.CreateUnbounded<ReadOnlyMemory<byte>>();
      var chunks  = 10240;

      for (var i = 0; i < chunks; i++)
      {
        var bytes  = new byte[chunks];
        var random = new Random();
        random.NextBytes(bytes);
        await channel.Writer.WriteAsync(bytes)
                     .ConfigureAwait(false);
      }

      channel.Writer.Complete();

      var (key, size) = await ObjectStorage!.AddOrUpdateAsync(new ObjectData
                                                              {
                                                                SessionId = "SessionId",
                                                                ResultId  = "ResultId",
                                                              },
                                                              channel.Reader.ReadAllAsync())
                                            .ConfigureAwait(false);

      var dict = await ObjectStorage!.GetSizesAsync(new[]
                                                    {
                                                      key,
                                                    }!)
                                     .ConfigureAwait(false);

      Assert.That(dict.Keys.Count,
                  Is.EqualTo(1));
      Assert.That(dict[key],
                  Is.EqualTo(size));
      Assert.That(dict[key],
                  Is.EqualTo(size));
    }
  }
}
