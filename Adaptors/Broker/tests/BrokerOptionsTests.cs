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

using System.Net;

using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.Broker.Tests;

/// <summary>
///   Validation of the adapter options at startup.
/// </summary>
[TestFixture]
public class BrokerOptionsTests
{
  private static void Build(Dictionary<string, string?> settings)
  {
    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(settings);
    new QueueBuilder().Build(new ServiceCollection(),
                             configuration,
                             NullLogger.Instance);
  }

  [TestCase("true",
            "0",
            false)]
  [TestCase("true",
            "-1",
            false)]
  [TestCase("true",
            "1024",
            true)]
  [TestCase("false",
            "0",
            true)]
  public void CacheCapacityIsRequiredWithAffinity(string affinity,
                                                  string capacity,
                                                  bool   valid)
  {
    var settings = new Dictionary<string, string?>
                   {
                     ["Broker:Endpoint"]           = "http://127.0.0.1:1",
                     ["Broker:Affinity"]           = affinity,
                     ["Broker:CacheCapacityBytes"] = capacity,
                   };
    if (valid)
    {
      Assert.DoesNotThrow(() => Build(settings));
    }
    else
    {
      Assert.Throws<InvalidOperationException>(() => Build(settings));
    }
  }

  [TestCase(Broker.IdSource.HostName)]
  [TestCase(Broker.IdSource.Ip)]
  public void NodeIdIsFoundFromItsSource(Broker.IdSource source)
    => Assert.That(NodeIdentity.Resolve(new Broker
                                        {
                                          NodeIdSource = source,
                                        }),
                   Is.EqualTo(source == Broker.IdSource.HostName
                                ? Dns.GetHostName()
                                : LocalIpFinder.LocalIpv4Address()));

  [Test]
  public void ExplicitNodeIdWinsOverItsSource()
    => Assert.That(NodeIdentity.Resolve(new Broker
                                        {
                                          NodeId       = "cache-group-1",
                                          NodeIdSource = Broker.IdSource.NodeName,
                                        }),
                   Is.EqualTo("cache-group-1"));

  [Test]
  [NonParallelizable]
  public void NodeNameIsReadFromTheEnvironment()
  {
    var previous = Environment.GetEnvironmentVariable("NODE_NAME");
    try
    {
      Environment.SetEnvironmentVariable("NODE_NAME",
                                         "k8s-node-3");
      Assert.That(NodeIdentity.Resolve(new Broker
                                       {
                                         NodeIdSource = Broker.IdSource.NodeName,
                                       }),
                  Is.EqualTo("k8s-node-3"));

      // Missing: fails at startup rather than falling back to another identity.
      Environment.SetEnvironmentVariable("NODE_NAME",
                                         null);
      Assert.Throws<InvalidOperationException>(() => Build(new Dictionary<string, string?>
                                                           {
                                                             ["Broker:Endpoint"]     = "http://127.0.0.1:1",
                                                             ["Broker:Affinity"]     = "true",
                                                             ["Broker:NodeIdSource"] = "NodeName",
                                                           }));
    }
    finally
    {
      Environment.SetEnvironmentVariable("NODE_NAME",
                                         previous);
    }
  }
}
