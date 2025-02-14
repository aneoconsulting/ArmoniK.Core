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

using ArmoniK.Core.Adapters.Amqp;
using ArmoniK.Core.Adapters.RabbitMQ;
using ArmoniK.Core.Common.DynamicLoading;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using PullQueueStorage = ArmoniK.Core.Adapters.RabbitMQ.PullQueueStorage;
using PushQueueStorage = ArmoniK.Core.Adapters.RabbitMQ.PushQueueStorage;

namespace ArmoniK.Core.Tests.Queue;

public class QueueStorageTests : QueueStorageTestsBase
{
  private static readonly string SolutionRoot =
    Path.GetFullPath(Path.Combine(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(Path.GetDirectoryName(typeof(QueueStorageTests)
                                                                                                                                                                      .Assembly
                                                                                                                                                                      .Location))))))) ??
                     string.Empty);

  private static readonly string RabbitPath =
    $"{Path.DirectorySeparatorChar}Adaptors{Path.DirectorySeparatorChar}RabbitMQ{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net8.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.RabbitMQ.dll";

  private static readonly string AmqpPath =
    $"{Path.DirectorySeparatorChar}Adaptors{Path.DirectorySeparatorChar}Amqp{Path.DirectorySeparatorChar}src{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}Debug{Path.DirectorySeparatorChar}net8.0{Path.DirectorySeparatorChar}ArmoniK.Core.Adapters.Amqp.dll";

  public static IEnumerable<TestCaseData> TestCasesQueueLocation
  {
    get
    {
      yield return new TestCaseData($"{SolutionRoot}{RabbitPath}",
                                    "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder").SetArgDisplayNames("RabbitMQ");
      yield return new TestCaseData($"{SolutionRoot}{AmqpPath}",
                                    "ArmoniK.Core.Adapters.Amqp.QueueBuilder").SetArgDisplayNames("Amqp");
    }
  }

  

  [Test]
  [TestCaseSource(nameof(TestCasesQueueLocation))]
  public void CreatePullQueueStorageShouldFail(string path,
                                               string className)
  {
    var config = new Dictionary<string, string?>
                 {
                   {
                     $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}", path
                   },
                   {
                     $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}", className
                   },
                   {
                     "RabbitMQ:User", "guest"
                   },
                   {
                     "Amqp:User", "guest"
                   },
                 };

    var serviceProvider = BuildServiceProvider(config);

    // Charger dynamiquement le client et les options
    var pullClient = GetPullClient(serviceProvider,
                                   className);
    var options = GetOptions(serviceProvider);

    var badOpt = CreateDefaultOptions();
    badOpt.PartitionId = "";

    switch (className)
    {
      case "RabbitMQ":
        Assert.Throws<ArgumentOutOfRangeException>(() =>
                                                   {
                                                     var _ = new PullQueueStorage(badOpt,
                                                                                  (IConnectionRabbit)pullClient,
                                                                                  NullLogger<PullQueueStorage>.Instance);
                                                   });
        break;
      case "Amqp":
        Assert.Throws<ArgumentOutOfRangeException>(() =>
                                                   {
                                                     var _ = new Adapters.Amqp.PullQueueStorage(badOpt,
                                                                                                (IConnectionAmqp)pullClient,
                                                                                                NullLogger<Adapters.Amqp.PullQueueStorage>.Instance);
                                                   });
        break;
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCasesQueueLocation))]
  public async Task GetQueueStorageInstanceShouldLoad(string path,
                                                      string className)
  {
    var config = new Dictionary<string, string?>
                 {
                   {
                     $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.AdapterAbsolutePath)}", path
                   },
                   {
                     $"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}", className
                   },
                   {
                     "RabbitMQ:User", "guest"
                   },
                   {
                     "Amqp:User", "guest"
                   },
                 };

    var serviceProvider = BuildServiceProvider(config);

    // Charger dynamiquement le client et les options
    var pullClient = GetPullClient(serviceProvider,
                                   className);
    var pushClient = GetPushClient(serviceProvider,
                                   className);
    var options = GetOptions(serviceProvider);

    // Utiliser le chargement dynamique pour obtenir l'instance de QueueStorage
    switch (className)
    {
      case "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder":
        var rabbitPullStorage = new PullQueueStorage(options,
                                                     (IConnectionRabbit)pullClient,
                                                     NullLogger<PullQueueStorage>.Instance);
        var rabbitPushStorage = new PushQueueStorage(options,
                                                     (IConnectionRabbit)pushClient,
                                                     NullLogger<PushQueueStorage>.Instance);
        Assert.NotNull(rabbitPullStorage);
        Assert.NotNull(rabbitPushStorage);
        Assert.IsInstanceOf<IConnectionRabbit>(rabbitPullStorage);
        Assert.IsInstanceOf<IConnectionRabbit>(rabbitPushStorage);
        break;
      case "ArmoniK.Core.Adapters.Amqp.QueueBuilder":
        var amqpPullStorage = new Adapters.Amqp.PullQueueStorage(options,
                                                                 (IConnectionAmqp)pullClient,
                                                                 NullLogger<Adapters.Amqp.PullQueueStorage>.Instance);
        var amqpPushStorage = new Adapters.Amqp.PushQueueStorage(options,
                                                                 (IConnectionAmqp)pushClient,
                                                                 NullLogger<Adapters.Amqp.PushQueueStorage>.Instance);
        Assert.NotNull(amqpPullStorage);
        Assert.NotNull(amqpPushStorage);
        Assert.IsInstanceOf<IConnectionAmqp>(amqpPullStorage);
        Assert.IsInstanceOf<IConnectionAmqp>(amqpPushStorage);
        break;
      default:
        throw new InvalidOperationException("Unknown queue adapter type.");
    }
  }


  private static IServiceProvider BuildServiceProvider(Dictionary<string, string?> config)
  {
    var loggerProvider = new ConsoleForwardingLoggerProvider();
    var logger         = loggerProvider.CreateLogger("root");

    AppDomain.CurrentDomain.AssemblyResolve += new CollocatedAssemblyResolver(logger).AssemblyResolve;

    var configuration = new ConfigurationManager();
    configuration.AddInMemoryCollection(config);

    var serviceCollection = new ServiceCollection();
    serviceCollection.AddAdapter(configuration,
                                 nameof(Components.QueueAdaptorSettings),
                                 logger);

    // Enregistrement des services spécifiques en fonction de la configuration
    var className = config[$"{Components.SettingSection}:{nameof(Components.QueueAdaptorSettings)}:{nameof(Components.QueueAdaptorSettings.ClassName)}"];

    if (className.Contains("RabbitMQ"))
    {
      serviceCollection.AddSingleton<IConnectionRabbit, SimpleRabbitClient>();
    }
    else if (className.Contains("Amqp"))
    {
      serviceCollection.AddSingleton<IConnectionAmqp, SimpleAmqpClient>();
    }
    else
    {
      throw new InvalidOperationException("Unknown queue adapter type.");
    }

    return serviceCollection.BuildServiceProvider();
  }

  private object GetPullClient(IServiceProvider serviceProvider,
                               string           className)
    => className switch
       {
         "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder" => serviceProvider.GetRequiredService<IConnectionRabbit>(),
         "ArmoniK.Core.Adapters.Amqp.QueueBuilder"     => serviceProvider.GetRequiredService<IConnectionAmqp>(),
         _                                             => throw new InvalidOperationException("Unknown queue adapter type."),
       };

  private object GetPushClient(IServiceProvider serviceProvider,
                               string           className)
    => className switch
       {
         "ArmoniK.Core.Adapters.RabbitMQ.QueueBuilder" => serviceProvider.GetRequiredService<IConnectionRabbit>(),
         "ArmoniK.Core.Adapters.Amqp.QueueBuilder"     => serviceProvider.GetRequiredService<IConnectionAmqp>(),
         _                                             => throw new InvalidOperationException("Unknown queue adapter type."),
       };

  private static Adapters.QueueCommon.Amqp GetOptions(IServiceProvider serviceProvider)
    => serviceProvider.GetRequiredService<Adapters.QueueCommon.Amqp>();
}
