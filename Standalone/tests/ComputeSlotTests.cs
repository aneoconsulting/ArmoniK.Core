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
using System.IO;

using ArmoniK.Api.Common.Options;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Standalone.Tests;

[TestFixture]
public class ComputeSlotTests
{
  [Test]
  public void ExpandShouldReplaceTheDataFolderAndTheSlot()
    => Assert.That(ComputeSlot.Expand("{data}/slot{slot}/worker.sock",
                                      "/var/armonik",
                                      3),
                   Is.EqualTo("/var/armonik/slot3/worker.sock"));

  [Test]
  public void SlotsShouldHaveTheirOwnSocketsAndCaches()
  {
    var options = new StandaloneOptions();

    var slot0 = ComputeSlot.SlotConfiguration(options,
                                              "/data",
                                              0);
    var slot1 = ComputeSlot.SlotConfiguration(options,
                                              "/data",
                                              1);

    Assert.Multiple(() =>
                    {
                      Assert.That(slot0["ComputePlane:WorkerChannel:Address"],
                                  Is.EqualTo("/data/slot0/worker.sock"));
                      Assert.That(slot0["ComputePlane:AgentChannel:Address"],
                                  Is.EqualTo("/data/slot0/agent.sock"));
                      Assert.That(slot0["ComputePlane:WorkerChannel:SocketType"],
                                  Is.EqualTo(nameof(GrpcSocketType.UnixDomainSocket)));
                      Assert.That(slot0["Pollster:SharedCacheFolder"],
                                  Is.EqualTo(Path.Combine("/data",
                                                          "slot0",
                                                          "shared")));
                      Assert.That(slot0["Pollster:InternalCacheFolder"],
                                  Is.EqualTo(Path.Combine("/data",
                                                          "slot0",
                                                          "internal")));
                      foreach (var (key, value) in slot0)
                      {
                        if (!key.EndsWith("SocketType"))
                        {
                          Assert.That(slot1[key],
                                      Is.Not.EqualTo(value),
                                      key);
                        }
                      }
                    });
  }

  [Test]
  public void TcpSlotsShouldUseTheAddressTemplates()
  {
    var options = new StandaloneOptions
                  {
                    SocketType    = GrpcSocketType.Tcp,
                    WorkerAddress = "http://localhost:1{slot}080",
                    AgentAddress  = "http://localhost:1{slot}081",
                  };

    var slot2 = ComputeSlot.SlotConfiguration(options,
                                              "/data",
                                              2);

    Assert.Multiple(() =>
                    {
                      Assert.That(slot2["ComputePlane:WorkerChannel:Address"],
                                  Is.EqualTo("http://localhost:12080"));
                      Assert.That(slot2["ComputePlane:AgentChannel:Address"],
                                  Is.EqualTo("http://localhost:12081"));
                      Assert.That(slot2["ComputePlane:AgentChannel:SocketType"],
                                  Is.EqualTo(nameof(GrpcSocketType.Tcp)));
                    });
  }

  [Test]
  public void TooLongSocketPathsShouldBeRejected()
  {
    var options = new StandaloneOptions();
    var dataFolder = Path.Combine(Path.GetTempPath(),
                                  new string('a',
                                             120));

    Assert.That(() => ComputeSlot.Build(0,
                                        options,
                                        dataFolder,
                                        new ConfigurationManager(),
                                        new ServiceCollection().BuildServiceProvider()),
                Throws.InstanceOf<InvalidOperationException>()
                      .With.Message.Contains(nameof(StandaloneOptions.DataFolder)));
  }
}
