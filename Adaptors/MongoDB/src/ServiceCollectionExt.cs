// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;

namespace ArmoniK.Core.Adapters.MongoDB;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddMongoComponents(
    this IServiceCollection services,
    ConfigurationManager    configuration,
    ILogger                 logger
  )
  {
    logger.LogInformation("Configure MongoDB client");

    var components = configuration.GetSection(Components.SettingSection);

    var isMongoRequired = false;

    if (components["TableStorage"] == "ArmoniK.Adapters.MongoDB.TableStorage")
    {
      services.AddOption<TableStorage>(configuration,
                                               TableStorage.SettingSection)
              .AddTransient<ITaskTable, TaskTable>()
              .AddTransient<ISessionTable, SessionTable>()
              .AddTransient<IDispatchTable, DispatchTable>()
              .AddTransient<IResultTable, ResultTable>();
      isMongoRequired = true;
    }

    if (components["QueueStorage"] == "ArmoniK.Adapters.MongoDB.LockedQueueStorage")
    {
      services.AddOption<QueueStorage>(configuration,
                                       QueueStorage.SettingSection)
              .AddTransientWithHealthCheck<LockedQueueStorage>($"MongoDB.{nameof(LockedQueueStorage)}")
              .AddTransientWithHealthCheck<IQueueStorage, LockedWrapperQueueStorage>($"MongoDB.{nameof(LockedWrapperQueueStorage)}")
              .AddTransient<ILockedQueueStorage, LockedQueueStorage>();

      isMongoRequired = true;
    }

    if (components["ObjectStorage"] == "ArmoniK.Adapters.MongoDB.ObjectStorage")
    {
      services.AddOption<Options.ObjectStorage>(configuration,
                                                Options.ObjectStorage.SettingSection)
              .AddTransient<ObjectStorage>()
              .AddTransient<IObjectStorage, ObjectStorage>();

      isMongoRequired = true;
    }

    if (isMongoRequired)
    {
      services.AddOption<Options.MongoDB>(configuration,
                                          Options.MongoDB.SettingSection,
                                          out var mongoOptions);

      using var _ = logger.BeginNamedScope("MongoDB configuration",
                                           ("host", mongoOptions.Host),
                                           ("port", mongoOptions.Port));

      if (string.IsNullOrEmpty(mongoOptions.Host))
        throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                              $"{nameof(Options.MongoDB.Host)} is not defined.");

      if (string.IsNullOrEmpty(mongoOptions.DatabaseName))
        throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                              $"{nameof(Options.MongoDB.DatabaseName)} is not defined.");

      if (!string.IsNullOrEmpty(mongoOptions.CredentialsPath))
      {
        configuration.AddJsonFile(mongoOptions.CredentialsPath);

        services.AddOption(configuration,
                           Options.MongoDB.SettingSection,
                           out mongoOptions);

        logger.LogTrace("Loaded mongodb credentials from file {path}",
                        mongoOptions.CredentialsPath);
      }
      else
      {
        logger.LogTrace("No credentials provided");
      }

      if (!string.IsNullOrEmpty(mongoOptions.CAFile))
      {
        var                  localTrustStore       = new X509Store(StoreName.Root);
        var certificateCollection = new X509Certificate2Collection();
        try
        {
          certificateCollection.ImportFromPemFile(mongoOptions.CAFile);
          localTrustStore.Open(OpenFlags.ReadWrite);
          localTrustStore.AddRange(certificateCollection);
          logger.LogTrace("Imported mongodb certificate from file {path}",
                          mongoOptions.CAFile);
        }
        catch (Exception ex)
        {
          logger.LogError("Root certificate import failed: {error}",
                          ex.Message);
          throw;
        }
        finally
        {
          localTrustStore.Close();
        }
      }

      string connectionString = null;
      if (string.IsNullOrEmpty(mongoOptions.User) || string.IsNullOrEmpty(mongoOptions.Password))
      {
        var template = "mongodb://{0}:{1}/{2}";
        connectionString = String.Format(template,
                                         mongoOptions.Host,
                                         mongoOptions.Port,
                                         mongoOptions.DatabaseName);
      }
      else
      {
        var template = "mongodb://{0}:{1}@{2}:{3}/{4}";
        connectionString = String.Format(template,
                                         mongoOptions.User,
                                         mongoOptions.Password,
                                         mongoOptions.Host,
                                         mongoOptions.Port,
                                         mongoOptions.DatabaseName);
      }

      var settings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
      settings.AllowInsecureTls = mongoOptions.AllowInsecureTls;
      settings.UseTls           = mongoOptions.Tls;
      settings.DirectConnection = mongoOptions.DirectConnection;
      settings.Scheme           = ConnectionStringScheme.MongoDB;

      services.AddTransient<IMongoClient>(provider =>
              {
                var logger = provider.GetRequiredService<ILogger<IMongoClient>>();


                //if (logger.IsEnabled(LogLevel.Trace))
                //{
                //  settings.ClusterConfigurator = cb =>
                //  {
                //    cb.Subscribe<CommandStartedEvent>(e =>
                //    {
                //      logger
                //        .LogTrace("{CommandName} - {Command}",
                //                  e.CommandName,
                //                  e.Command.ToJson());
                //    });
                //  };
                //}

                return new MongoClient(settings);
              })
              .AddTransient(provider => provider.GetRequiredService<IMongoClient>().GetDatabase(mongoOptions.DatabaseName))
              .AddSingleton(typeof(MongoCollectionProvider<,>))
              .AddSingletonWithHealthCheck<SessionProvider>($"MongoDB.{nameof(SessionProvider)}")
              .AddHealthChecks()
              .AddMongoDb(settings,
                          mongoOptions.DatabaseName,
                          "MongoDb Connection",
                          tags: new[] { nameof(HealthCheckTag.Startup), nameof(HealthCheckTag.Liveness), nameof(HealthCheckTag.Readiness) });

      logger.LogInformation("MongoDB configuration complete");
    }

    return services;
  }
}