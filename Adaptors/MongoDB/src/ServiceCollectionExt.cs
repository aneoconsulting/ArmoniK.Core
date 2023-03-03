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

using System;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Extensions.DiagnosticSources;

namespace ArmoniK.Core.Adapters.MongoDB;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddMongoComponents(this IServiceCollection services,
                                                      ConfigurationManager    configuration,
                                                      ILogger                 logger)
  {
    Console.WriteLine("mongo 1");

    services.AddMongoClient(configuration,
                            logger);
    Console.WriteLine("mongo 2");
    services.AddMongoStorages(configuration,
                              logger);
    Console.WriteLine("mongo 3");

    return services;
  }

  [PublicAPI]
  public static IServiceCollection AddMongoStorages(this IServiceCollection services,
                                                    ConfigurationManager    configuration,
                                                    ILogger                 logger)
  {
    Console.WriteLine("AddMongoStorages 1");

    logger.LogInformation("Configure MongoDB Components");

    var components = configuration.GetSection(Components.SettingSection);
    Console.WriteLine("AddMongoStorages 2");

    if (components["TableStorage"] == "ArmoniK.Adapters.MongoDB.TableStorage")
    {
      Console.WriteLine("AddMongoStorages 3");

      services.AddOption<TableStorage>(configuration,
                                       TableStorage.SettingSection)
              .AddTransient<ITaskTable, TaskTable>()
              .AddTransient<ISessionTable, SessionTable>()
              .AddTransient<IResultTable, ResultTable>()
              .AddTransient<IPartitionTable, PartitionTable>()
              .AddTransient<ITaskWatcher, TaskWatcher>()
              .AddTransient<IResultWatcher, ResultWatcher>();
    }

    if (components["ObjectStorage"] == "ArmoniK.Adapters.MongoDB.ObjectStorage")
    {
      Console.WriteLine("AddMongoStorages 4");

      services.AddOption<Options.ObjectStorage>(configuration,
                                                Options.ObjectStorage.SettingSection)
              .AddTransient<ObjectStorageFactory>()
              .AddTransient<IObjectStorageFactory, ObjectStorageFactory>();
    }
    Console.WriteLine("AddMongoStorages 5");

    services.AddOption<Options.MongoDB>(configuration,
                                        Options.MongoDB.SettingSection,
                                        out var mongoOptions);
    Console.WriteLine("AddMongoStorages 6");

    services.AddTransient(provider => provider.GetRequiredService<IMongoClient>()
                                              .GetDatabase(mongoOptions.DatabaseName))
            .AddSingleton(typeof(MongoCollectionProvider<,>))
            .AddSingletonWithHealthCheck<SessionProvider>($"MongoDB.{nameof(SessionProvider)}");
    Console.WriteLine("AddMongoStorages 7");

    return services;
  }

  public static IServiceCollection AddMongoClient(this IServiceCollection services,
                                                  ConfigurationManager    configuration,
                                                  ILogger                 logger)
  {
    Console.WriteLine("AddMongoClient 1");

    Options.MongoDB mongoOptions;
    services.AddOption(configuration,
                       Options.MongoDB.SettingSection,
                       out mongoOptions);
    Console.WriteLine("AddMongoClient 2");

    using var _ = logger.BeginNamedScope("MongoDB configuration",
                                         ("host", mongoOptions.Host),
                                         ("port", mongoOptions.Port));
    Console.WriteLine("AddMongoClient 3");

    if (string.IsNullOrEmpty(mongoOptions.Host))
    {
      Console.WriteLine("AddMongoClient 4");
      throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                            $"{nameof(Options.MongoDB.Host)} is not defined.");
    }

    if (string.IsNullOrEmpty(mongoOptions.DatabaseName))
    {
      Console.WriteLine("AddMongoClient 5");

      throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                            $"{nameof(Options.MongoDB.DatabaseName)} is not defined.");
    }

    if (!string.IsNullOrEmpty(mongoOptions.CredentialsPath))
    {
      Console.WriteLine("AddMongoClient 6");

      configuration.AddJsonFile(mongoOptions.CredentialsPath,
                                false,
                                false);
      Console.WriteLine("AddMongoClient 7");

      services.AddOption(configuration,
                         Options.MongoDB.SettingSection,
                         out mongoOptions);
      Console.WriteLine("AddMongoClient 8");

      logger.LogTrace("Loaded mongodb credentials from file {path}",
                      mongoOptions.CredentialsPath);
    }
    else
    {
      logger.LogTrace("No credentials provided");
    }

    if (!string.IsNullOrEmpty(mongoOptions.CAFile))
    {
      Console.WriteLine("AddMongoClient 9");

      var localTrustStore       = new X509Store(StoreLocation.CurrentUser);
      var certificateCollection = new X509Certificate2Collection();
      try
      {
        Console.WriteLine("AddMongoClient 10");
        Console.WriteLine($"Try to mount Mongo ca file from : {mongoOptions.CAFile}");

        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
          mongoOptions.CAFile = @"c:\temp\mongodb\chain.pem";
          Console.WriteLine($"ok we are on windows, I hack this to : {mongoOptions.CAFile}");
        }


        certificateCollection.ImportFromPemFile(@"c:\temp\mongodb\chain.pem");

        foreach(X509Certificate2 certificate in certificateCollection)
        {
          Console.WriteLine($"{certificate.Subject} {certificate.Issuer} {certificate.Version}");
        }

        Console.WriteLine("AddMongoClient 11");

        localTrustStore.Open(OpenFlags.ReadWrite);
        Console.WriteLine("AddMongoClient 12");

        localTrustStore.AddRange(certificateCollection);
        Console.WriteLine("AddMongoClient 13");

        logger.LogTrace("Imported mongodb certificate from file {path}",
                        mongoOptions.CAFile);
        Console.WriteLine("AddMongoClient 14");

      }
      catch (Exception ex)
      {
        Console.WriteLine("AddMongoClient 15");

        logger.LogError("Root certificate import failed: {error}",
                        ex.Message);
        throw;
      }
      finally
      {
        localTrustStore.Close();
      }
    }

    string connectionString;
    if (string.IsNullOrEmpty(mongoOptions.User) || string.IsNullOrEmpty(mongoOptions.Password))
    {
      var template = "mongodb://{0}:{1}/{2}";
      connectionString = string.Format(template,
                                       mongoOptions.Host,
                                       mongoOptions.Port,
                                       mongoOptions.DatabaseName);
    }
    else
    {
      var template = "mongodb://{0}:{1}@{2}:{3}/{4}";
      connectionString = string.Format(template,
                                       mongoOptions.User,
                                       mongoOptions.Password,
                                       mongoOptions.Host,
                                       mongoOptions.Port,
                                       mongoOptions.DatabaseName);
    }

    var settings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
    settings.AllowInsecureTls       = mongoOptions.AllowInsecureTls;
    settings.UseTls                 = mongoOptions.Tls;
    settings.DirectConnection       = mongoOptions.DirectConnection;
    settings.Scheme                 = ConnectionStringScheme.MongoDB;
    settings.MaxConnectionPoolSize  = mongoOptions.MaxConnectionPoolSize;
    settings.ServerSelectionTimeout = mongoOptions.ServerSelectionTimeout;

    services.AddTransient<IMongoClient>(_ =>
                                        {
                                          settings.ClusterConfigurator = cb =>
                                                                         {
                                                                           //cb.Subscribe<CommandStartedEvent>(e => logger.LogTrace("{CommandName} - {Command}",
                                                                           //                                                       e.CommandName,
                                                                           //                                                       e.Command.ToJson()));
                                                                           cb.Subscribe(new DiagnosticsActivityEventSubscriber());
                                                                         };

                                          return new MongoClient(settings);
                                        });

    logger.LogInformation("MongoDB configuration complete");

    logger.LogDebug("{Option} {Value}",
                    nameof(mongoOptions.MaxConnectionPoolSize),
                    mongoOptions.MaxConnectionPoolSize);
    logger.LogDebug("{Option} {Value}",
                    nameof(mongoOptions.DataRetention),
                    mongoOptions.DataRetention);

    return services;
  }

  /// <summary>
  ///   Add the storage provider for the client authentication system to the service collection
  /// </summary>
  /// <param name="services">Services</param>
  /// <param name="configuration">Configuration</param>
  /// <param name="logger">Logger</param>
  /// <returns>Services</returns>
  [PublicAPI]
  public static IServiceCollection AddClientSubmitterAuthenticationStorage(this IServiceCollection services,
                                                                           ConfigurationManager    configuration,
                                                                           ILogger                 logger)
  {
    var components = configuration.GetSection(Components.SettingSection);
    if (components[nameof(Components.AuthenticationStorage)] == "ArmoniK.Adapters.MongoDB.AuthenticationTable")
    {
      services.TryAddSingleton(typeof(MongoCollectionProvider<,>));
      services.AddTransient<IAuthenticationTable, AuthenticationTable>();
    }

    return services;
  }

  /// <summary>
  ///   Add the authentication service to the service collection
  /// </summary>
  /// <param name="services">Services</param>
  /// <param name="configuration">Configuration</param>
  /// <param name="logger">Logger</param>
  /// <param name="authCache">Created authentication cache</param>
  /// <returns>Services</returns>
  [PublicAPI]
  public static IServiceCollection AddClientSubmitterAuthServices(this IServiceCollection services,
                                                                  ConfigurationManager    configuration,
                                                                  ILogger                 logger,
                                                                  out AuthenticationCache authCache)
  {
    authCache = new AuthenticationCache();
    services.Configure<AuthenticatorOptions>(configuration.GetSection(AuthenticatorOptions.SectionName))
            .AddSingleton(authCache)
            .AddAuthentication()
            .AddScheme<AuthenticatorOptions, Authenticator>(Authenticator.SchemeName,
                                                            _ =>
                                                            {
                                                            });
    services.AddSingleton<IAuthorizationPolicyProvider, AuthorizationPolicyProvider>()
            .AddAuthorization();
    return services;
  }
}
