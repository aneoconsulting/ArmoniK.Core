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

using System.Collections;
using System.Collections.Generic;
using System.IO;

using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Tests.Helpers;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.AdapterLoading;

[TestFixture]
public class AdapterLoadingTest
{
  private static readonly string SolutionRoot =
    Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(typeof(AdapterLoadingTest)
                                                                                                                                                                      .Assembly
                                                                                                                                                                      .Location)))))) ??
                                  string.Empty));

  private static readonly string AmqpPath =
    $"{Path.DirectorySeparatorChar}Adaptors{Path.DirectorySeparatorChar}Amqp{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net6.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.Amqp.dll";

  private static readonly string RabbitPath =
    $"{Path.DirectorySeparatorChar}Adaptors{Path.DirectorySeparatorChar}RabbitMQ{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net6.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.RabbitMQ.dll";


  public static IEnumerable TestCasesQueueLocation
  {
    get
    {
      yield return new TestCaseData($"{SolutionRoot}{AmqpPath}",
                                    "ArmoniK.Core.Adapters.Amqp.QueueBuilder").SetArgDisplayNames("Amqp");
      yield return new TestCaseData($"{SolutionRoot}{RabbitPath}",
                                    "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder").SetArgDisplayNames("RabbitMQ");
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCasesQueueLocation))]
  public void QueueShouldLoad(string path,
                              string className)
  {
    var loggerProvider = new ConsoleForwardingLoggerProvider();
    var logger         = loggerProvider.CreateLogger("root");

    Dictionary<string, string?> minimalConfig = new()
                                                {
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.QueueStorage)}:{nameof(Components.QueueStorage.AdapterAbsolutePath)}",
                                                    path
                                                  },
                                                  {
                                                    $"{Components.SettingSection}:{nameof(Components.QueueStorage)}:{nameof(Components.QueueStorage.ClassName)}",
                                                    className
                                                  },
                                                  {
                                                    "Amqp:User", "User"
                                                  },
                                                };

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(minimalConfig);

    var serviceCollection = new ServiceCollection();
    serviceCollection.AddQueue(configuration,
                               logger);

    Assert.DoesNotThrow(() => serviceCollection.BuildServiceProvider());
  }
}
