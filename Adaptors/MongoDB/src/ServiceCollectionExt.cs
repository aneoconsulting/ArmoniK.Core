// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Linq;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Options;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Core.Extensions.DiagnosticSources;

namespace ArmoniK.Core.Adapters.MongoDB;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddMongoComponents(this IServiceCollection services,
                                                      ConfigurationManager    configuration,
                                                      ILogger                 logger)
  {
    services.AddMongoClient(configuration,
                            logger);
    services.AddMongoStorages(configuration,
                              logger);
    return services;
  }

  [PublicAPI]
  public static IServiceCollection AddMongoStorages(this IServiceCollection services,
                                                    ConfigurationManager    configuration,
                                                    ILogger                 logger)
  {
    logger.LogInformation("Configure MongoDB Components");

    var components = configuration.GetSection(Components.SettingSection);

    if (components["TableStorage"] == "ArmoniK.Adapters.MongoDB.TableStorage")
    {
      services.AddOption<TableStorage>(configuration,
                                       TableStorage.SettingSection)
              .AddSingleton<ITaskTable, TaskTable>()
              .AddSingleton<ISessionTable, SessionTable>()
              .AddSingleton<IResultTable, ResultTable>()
              .AddSingleton<IPartitionTable, PartitionTable>()
              .AddSingleton<ITaskWatcher, TaskWatcher>()
              .AddSingleton<IResultWatcher, ResultWatcher>();
    }

    if (components["ObjectStorage"] == "ArmoniK.Adapters.MongoDB.ObjectStorage")
    {
      services.AddOption<Options.ObjectStorage>(configuration,
                                                Options.ObjectStorage.SettingSection)
              .AddSingleton<ObjectStorage>()
              .AddSingleton<IObjectStorage, ObjectStorage>();
    }

    services.AddOption<Options.MongoDB>(configuration,
                                        Options.MongoDB.SettingSection,
                                        out var mongoOptions);

    services.AddSingleton(provider => provider.GetRequiredService<IMongoClient>()
                                              .GetDatabase(mongoOptions.DatabaseName))
            .AddSingleton(typeof(MongoCollectionProvider<,>))
            .AddSingletonWithHealthCheck<SessionProvider>($"MongoDB.{nameof(SessionProvider)}");

    return services;
  }

  public static IServiceCollection AddMongoClient(this IServiceCollection services,
                                                  ConfigurationManager    configuration,
                                                  ILogger                 logger)
  {
    Options.MongoDB mongoOptions;
    services.AddOption(configuration,
                       Options.MongoDB.SettingSection,
                       out mongoOptions);

    using var _ = logger.BeginNamedScope("MongoDB configuration",
                                         ("host", mongoOptions.Hosts));

    if (!mongoOptions.Hosts.Any())
    {
      throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                            $"{nameof(Options.MongoDB.Hosts)} is not defined.");
    }

    if (string.IsNullOrEmpty(mongoOptions.DatabaseName))
    {
      throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                            $"{nameof(Options.MongoDB.DatabaseName)} is not defined.");
    }

    if (!string.IsNullOrEmpty(mongoOptions.CredentialsPath))
    {
      configuration.AddJsonFile(mongoOptions.CredentialsPath,
                                false,
                                false);

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
      var localTrustStore       = new X509Store(StoreName.Root);
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

    var url = new MongoUrlBuilder
              {
                Servers = mongoOptions.Hosts.Select(MongoServerAddress.Parse),
              };
    if (!string.IsNullOrEmpty(mongoOptions.User))
    {
      url.Username = mongoOptions.User;
    }

    if (!string.IsNullOrEmpty(mongoOptions.Password))
    {
      url.Password = mongoOptions.Password;
    }

    url.Scheme                 = mongoOptions.Scheme;
    url.DirectConnection       = mongoOptions.DirectConnection;
    url.UseTls                 = mongoOptions.Tls;
    url.AllowInsecureTls       = mongoOptions.AllowInsecureTls;
    url.MaxConnectionPoolSize  = mongoOptions.MaxConnectionPoolSize;
    url.ReplicaSetName         = mongoOptions.ReplicaSet;
    url.ServerSelectionTimeout = mongoOptions.ServerSelectionTimeout;

    var settings = MongoClientSettings.FromUrl(url.ToMongoUrl());
    settings.ClusterConfigurator = cb =>
                                   {
                                     //cb.Subscribe<CommandStartedEvent>(e => logger.LogTrace("{CommandName} - {Command}",
                                     //                                                       e.CommandName,
                                     //                                                       e.Command.ToJson()));
                                     cb.Subscribe(new DiagnosticsActivityEventSubscriber());
                                   };
    if (mongoOptions.ClientCertificateFiles.Any())
    {
      settings.SslSettings = new SslSettings
                             {
                               ClientCertificates = mongoOptions.ClientCertificateFiles.Select(s => X509Certificate2.CreateFromPemFile(s)),
                             };
    }

    var client = new MongoClient(settings);

    services.AddSingleton<IMongoClient>(client);

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
  /// <returns>Services</returns>
  [PublicAPI]
  public static IServiceCollection AddClientSubmitterAuthenticationStorage(this IServiceCollection services,
                                                                           ConfigurationManager    configuration)
  {
    var components = configuration.GetSection(Components.SettingSection);
    if (components[nameof(Components.AuthenticationStorage)] == "ArmoniK.Adapters.MongoDB.AuthenticationTable")
    {
      services.TryAddSingleton(typeof(MongoCollectionProvider<,>));
      services.AddSingleton<IAuthenticationTable, AuthenticationTable>();
    }

    return services;
  }

  /// <summary>
  ///   Add the authentication service to the service collection
  /// </summary>
  /// <param name="services">Services</param>
  /// <param name="configuration">Configuration</param>
  /// <param name="authCache">Created authentication cache</param>
  /// <returns>Services</returns>
  [PublicAPI]
  public static IServiceCollection AddClientSubmitterAuthServices(this IServiceCollection services,
                                                                  ConfigurationManager    configuration,
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
