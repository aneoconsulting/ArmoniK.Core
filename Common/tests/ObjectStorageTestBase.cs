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
using System.Threading.Tasks;

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.Storage;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture]
public class ObjectStorageTestBase
{
  /* Interface to test */
  protected IObjectStorage ObjectStorage;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of TaskTable to the corresponding interface implementation */
  public virtual void GetObjectStorageInstance()
  {
  }

  [SetUp]
  public void SetUp()
  {
    GetObjectStorageInstance();

    if (!RunTests)
      return;
    var dataBytesList = new List<byte[]>();
    dataBytesList.Add(Encoding.ASCII.GetBytes("AAAA"));
    dataBytesList.Add(Encoding.ASCII.GetBytes("BBBB"));
    dataBytesList.Add(Encoding.ASCII.GetBytes("CCCC"));
    dataBytesList.Add(Encoding.ASCII.GetBytes("DDDD"));
    ObjectStorage.AddOrUpdateAsync("dataKey1", dataBytesList.ToAsyncEnumerable()).Wait();

    dataBytesList = new List<byte[]>();
    dataBytesList.Add(Encoding.ASCII.GetBytes("AAAABBBB"));
    ObjectStorage.AddOrUpdateAsync("dataKey2",
                                   dataBytesList.ToAsyncEnumerable()).Wait();

    dataBytesList = new List<byte[]>();
    ObjectStorage.AddOrUpdateAsync("dataKeyEmpty",
                                   dataBytesList.ToAsyncEnumerable()).Wait();
  }

  [TearDown]
  public virtual void TearDown()
  {
    ObjectStorage = null;
    RunTests      = false;
  }

  [Test]
  public async Task GetValuesAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var res = ObjectStorage.GetValuesAsync("dataKey1");
      Assert.AreEqual(4, await res.CountAsync());
    }
  }

  [Test]
  public void GetValuesAsyncShouldFail()
  {
    if (RunTests)
    {
      var res = ObjectStorage.GetValuesAsync("dataKeyNotExist");
      Assert.ThrowsAsync<ArmoniKException>(async () => await res.FirstAsync());
    }
  }

  [Test]
  public async Task PayloadShouldBeEqual()
  {
    if (RunTests)
    {
      var res = ObjectStorage.GetValuesAsync("dataKey2");
      var data = await res.SingleAsync();
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
      var res  = ObjectStorage.GetValuesAsync("dataKey1");
      // var data = await res.AggregateAsync((bytes1, bytes2) => bytes1.Concat(bytes2).ToArray());
      var data     = new List<byte>();
      foreach (var item in await res.ToListAsync())
      {
        data.AddRange(item);
      }

      var str  = Encoding.ASCII.GetString(data.ToArray());
      Console.WriteLine(str);
      Assert.IsTrue(str.SequenceEqual("AAAABBBBCCCCDDDD"));
    }
  }

  [Test]
  public async Task EmptyPayload()
  {
    if (RunTests)
    {
      var res = ObjectStorage.GetValuesAsync("dataKeyEmpty");
      Console.WriteLine(await res.CountAsync());
      var data = new List<byte>();
      foreach (var item in await res.ToListAsync())
      {
        data.AddRange(item);
      }

      var str = Encoding.ASCII.GetString(data.ToArray());
      Console.WriteLine(str);
      Assert.IsTrue(str.SequenceEqual(""));
    }
  }
}