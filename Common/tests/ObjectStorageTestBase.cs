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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class ObjectStorageTestBase
{
  [SetUp]
  public void SetUp()
  {
    GetObjectStorageInstance();

    if (!RunTests)
    {
      return;
    }

    var dataBytesList = new List<byte[]>();
    dataBytesList.Add(Encoding.ASCII.GetBytes("AAAA"));
    dataBytesList.Add(Encoding.ASCII.GetBytes("BBBB"));
    dataBytesList.Add(Encoding.ASCII.GetBytes("CCCC"));
    dataBytesList.Add(Encoding.ASCII.GetBytes("DDDD"));
    ObjectStorage!.AddOrUpdateAsync("dataKey1",
                                    dataBytesList.ToAsyncEnumerable())
                  .Wait();

    dataBytesList = new List<byte[]>();
    dataBytesList.Add(Encoding.ASCII.GetBytes("AAAABBBB"));
    ObjectStorage.AddOrUpdateAsync("dataKey2",
                                   dataBytesList.ToAsyncEnumerable())
                 .Wait();

    dataBytesList = new List<byte[]>();
    dataBytesList.Add(Array.Empty<byte>());
    ObjectStorage.AddOrUpdateAsync("dataKeyEmpty",
                                   dataBytesList.ToAsyncEnumerable())
                 .Wait();
  }

  [TearDown]
  public virtual void TearDown()
  {
    ObjectStorage        = null;
    ObjectStorageFactory = null;
    RunTests             = false;
  }

  /* Interface to test */
  protected IObjectStorage?        ObjectStorage;
  protected IObjectStorageFactory? ObjectStorageFactory;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of TaskTable to the corresponding interface implementation */
  public virtual void GetObjectStorageInstance()
  {
  }

  [Test]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ObjectStorageFactory!.Check(HealthCheckTag.Liveness)
                                                     .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ObjectStorageFactory.Check(HealthCheckTag.Readiness)
                                                    .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await ObjectStorageFactory.Check(HealthCheckTag.Startup)
                                                    .ConfigureAwait(false)).Status);

      await ObjectStorageFactory.Init(CancellationToken.None)
                                .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await ObjectStorageFactory.Check(HealthCheckTag.Liveness)
                                                 .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ObjectStorageFactory.Check(HealthCheckTag.Readiness)
                                                 .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await ObjectStorageFactory.Check(HealthCheckTag.Startup)
                                                 .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  public async Task AddValuesAsyncWithoutChunkShouldWork()
  {
    if (RunTests)
    {
      await ObjectStorage!.AddOrUpdateAsync("dataKeyNoChunk",
                                            AsyncEnumerable.Empty<byte[]>())
                          .ConfigureAwait(false);
      var data = new List<byte>();
      await foreach (var chunk in ObjectStorage!.GetValuesAsync("dataKeyNoChunk")
                                                .ConfigureAwait(false))
      {
        data.AddRange(chunk);
      }

      Assert.AreEqual(0,
                      data.Count);
    }
  }

  [Test]
  public void GetValuesAsyncShouldFail()
  {
    if (RunTests)
    {
      var res = ObjectStorage!.GetValuesAsync("dataKeyNotExist");
      Assert.ThrowsAsync<ObjectDataNotFoundException>(async () => await res.FirstAsync()
                                                                           .ConfigureAwait(false));
    }
  }

  [Test]
  public async Task PayloadShouldBeEqual()
  {
    if (RunTests)
    {
      var res = ObjectStorage!.GetValuesAsync("dataKey2");
      var data = await res.SingleAsync()
                          .ConfigureAwait(false);
      var str = Encoding.ASCII.GetString(data);
      Console.WriteLine(str);
      Assert.IsTrue(str.SequenceEqual("AAAABBBB"));
    }
  }

  [Test]
  public async Task Payload2ShouldBeEqual()
  {
    if (RunTests)
    {
      var res = ObjectStorage!.GetValuesAsync("dataKey1");
      // var data = await res.AggregateAsync((bytes1, bytes2) => bytes1.Concat(bytes2).ToArray());
      var data = new List<byte>();
      foreach (var item in await res.ToListAsync()
                                    .ConfigureAwait(false))
      {
        data.AddRange(item);
      }

      var str = Encoding.ASCII.GetString(data.ToArray());
      Console.WriteLine(str);
      Assert.IsTrue(str.SequenceEqual("AAAABBBBCCCCDDDD"));
    }
  }

  [Test]
  public async Task EmptyPayload()
  {
    if (RunTests)
    {
      var res = await ObjectStorage!.GetValuesAsync("dataKeyEmpty")
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
      Assert.IsTrue(str.SequenceEqual(""));
    }
  }

  [Test]
  public async Task DeleteKeysAndGetValuesAsyncShouldFail()
  {
    if (RunTests)
    {
      var listChunks = new List<byte[]>
                       {
                         Encoding.ASCII.GetBytes("Armonik Payload chunk"),
                         Encoding.ASCII.GetBytes("Data 1"),
                         Encoding.ASCII.GetBytes("Data 2"),
                         Encoding.ASCII.GetBytes("Data 3"),
                         Encoding.ASCII.GetBytes("Data 4"),
                       };

      await ObjectStorage!.AddOrUpdateAsync("dataKey",
                                            listChunks.ToAsyncEnumerable())
                          .ConfigureAwait(false);

      var res = await ObjectStorage!.GetValuesAsync("dataKey")
                                    .ToListAsync()
                                    .ConfigureAwait(false);

      Assert.AreEqual(listChunks.Count,
                      res.Count);

      Assert.AreEqual(string.Join("",
                                  listChunks.Select(chunk => Encoding.ASCII.GetString(chunk))),
                      string.Join("",
                                  res.Select(chunk => Encoding.ASCII.GetString(chunk))));

      await ObjectStorage!.TryDeleteAsync("dataKey")
                          .ConfigureAwait(false);

      Assert.ThrowsAsync<ObjectDataNotFoundException>(async () => await ObjectStorage!.GetValuesAsync("dataKey")
                                                                                      .FirstAsync()
                                                                                      .ConfigureAwait(false));
    }
  }
}
