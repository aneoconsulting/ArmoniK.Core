// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

// ReSharper disable once RedundantUsingDirective

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace AdapterLoading.Tests;

[TestFixture]
public class AdapterLoadingTest
{
  private static readonly string SolutionRoot =
    Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(typeof(AdapterLoadingTest)
                                                                                                                                                                                            .Assembly
                                                                                                                                                                                            .Location))))))) ??
                                  string.Empty));

  private static readonly string AmqpPath =
    $"{Path.DirectorySeparatorChar}Adaptors{Path.DirectorySeparatorChar}Amqp{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net6.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.Amqp.dll";

  private static readonly string RabbitPath =
    $"{Path.DirectorySeparatorChar}Adaptors{Path.DirectorySeparatorChar}RabbitMQ{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net6.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.RabbitMQ.dll";

  private static readonly string HelloPluginPath =
    $"{Path.DirectorySeparatorChar}..{Path.DirectorySeparatorChar}HelloPlugin{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net6.0{Path.DirectorySeparatorChar}HelloPlugin.dll";

  private static readonly string PluginPath =
    $"{Path.DirectorySeparatorChar}..{Path.DirectorySeparatorChar}ArmoniK.Contrib.Plugin.QueueSessionEquity{Path.DirectorySeparatorChar}Plugin{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net6.0{Path.DirectorySeparatorChar}Plugin.dll";


  public static IEnumerable TestCasesQueueLocation
  {
    get
    {
      yield return new TestCaseData($"{SolutionRoot}{AmqpPath}",
                                    "ArmoniK.Core.Adapters.Amqp.QueueBuilder").SetArgDisplayNames("Amqp");
      yield return new TestCaseData($"{SolutionRoot}{RabbitPath}",
                                    "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder").SetArgDisplayNames("RabbitMQ");
      yield return new TestCaseData($"{SolutionRoot}{HelloPluginPath}",
                                    "HelloPlugin.HelloWorld").SetArgDisplayNames("HelloPlugin");
      yield return new TestCaseData($"{SolutionRoot}{PluginPath}",
                                    "Plugin.QueueBuilder").SetArgDisplayNames("Plugin");
    }
  }

  public void Setup(Dictionary<string, string?> config)
  {
    var loggerProvider = new ConsoleForwardingLoggerProvider();
    var logger         = loggerProvider.CreateLogger("root");

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(config);

    var serviceCollection = new ServiceCollection();
    serviceCollection.AddQueue(configuration,
                               logger);

    serviceCollection.BuildServiceProvider();
  }

  [Test]
  [TestCaseSource(nameof(TestCasesQueueLocation))]
  public void QueueShouldLoad(string path,
                              string className)
  {
    Dictionary<string, string?> config = new()
                                         {
                                           {
                                             $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}",
                                             path
                                           },
                                           {
                                             $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}",
                                             className
                                           },
                                           {
                                             "Amqp:User", "User"
                                           },
                                         };

    Assert.DoesNotThrow(() => Setup(config));
  }

  public static IEnumerable ConfInvalidOperationException
  {
    get
    {
      yield return new TestCaseData(new Dictionary<string, string?>
                                    {
                                      {
                                        "Amqp:User", "User"
                                      },
                                    }).SetArgDisplayNames("No components");
      yield return new TestCaseData(new Dictionary<string, string?>
                                    {
                                      {
                                        "Amqp:User", "User"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}", ""
                                      },
                                    }).SetArgDisplayNames("Empty class");
      yield return new TestCaseData(new Dictionary<string, string?>
                                    {
                                      {
                                        "Amqp:User", "User"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}", ""
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}",
                                        "path"
                                      },
                                    }).SetArgDisplayNames("Empty path");
      yield return new TestCaseData(new Dictionary<string, string?>
                                    {
                                      {
                                        "Amqp:User", "User"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}",
                                        "invalid class"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}",
                                        "invalid path"
                                      },
                                    }).SetArgDisplayNames("invalid path");

      yield return new TestCaseData(new Dictionary<string, string?>
                                    {
                                      {
                                        "Amqp:User", "User"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}",
                                        "ArmoniK.Core.Adapters.Amqp.ConnectionAmqp"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}",
                                        $"{SolutionRoot}{AmqpPath}"
                                      },
                                    }).SetArgDisplayNames("Not implemented");
    }
  }

  [Test]
  [TestCaseSource(nameof(ConfInvalidOperationException))]
  public void InvalidConfShouldFail(Dictionary<string, string?> config)
    => Assert.Throws<InvalidOperationException>(() => Setup(config));

  public static IEnumerable ConfTypeLoadException
  {
    get
    {
      yield return new TestCaseData(new Dictionary<string, string?>
                                    {
                                      {
                                        "Amqp:User", "User"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}",
                                        "invalid class"
                                      },
                                      {
                                        $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}",
                                        $"{SolutionRoot}{AmqpPath}"
                                      },
                                    }).SetArgDisplayNames("invalid class");
    }
  }

  [Test]
  [TestCaseSource(nameof(ConfTypeLoadException))]
  public void InvalidTypeConfShouldFail(Dictionary<string, string?> config)
    => Assert.Throws<TypeLoadException>(() => Setup(config));
}
