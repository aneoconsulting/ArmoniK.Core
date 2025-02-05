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

using System;
using System.Security.Authentication;

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
using MongoDB.Driver.Core.Configuration;
using MongoDB.Driver.Core.Extensions.DiagnosticSources;

using static ArmoniK.Core.Utils.CertificateValidator;

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
                                         ("host", mongoOptions.Host),
                                         ("port", mongoOptions.Port));

    if (string.IsNullOrEmpty(mongoOptions.Host))
    {
      throw new ArgumentOutOfRangeException(Options.MongoDB.SettingSection,
                                            $"{nameof(Options.MongoDB.Host)} is not defined.");
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

    if (!string.IsNullOrEmpty(mongoOptions.AuthSource))
    {
      connectionString = $"{connectionString}?authSource={mongoOptions.AuthSource}";
    }

    var settings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));

    // Configure the connection settings
    settings.AllowInsecureTls       = mongoOptions.AllowInsecureTls;
    settings.UseTls                 = mongoOptions.Tls;
    settings.DirectConnection       = mongoOptions.DirectConnection;
    settings.Scheme                 = ConnectionStringScheme.MongoDB;
    settings.MaxConnectionPoolSize  = mongoOptions.MaxConnectionPoolSize;
    settings.ServerSelectionTimeout = mongoOptions.ServerSelectionTimeout;
    settings.ReplicaSetName         = mongoOptions.ReplicaSet;

    if (!string.IsNullOrEmpty(mongoOptions.CAFile))
    {
      var validationCallback = CreateCallback(mongoOptions.CAFile,
                                              mongoOptions.AllowInsecureTls,
                                              logger);

      settings.SslSettings = new SslSettings
                             {
                               EnabledSslProtocols                 = SslProtocols.Tls12,
                               ServerCertificateValidationCallback = validationCallback,
                             };
    }

    settings.ClusterConfigurator = cb =>
                                   {
                                     //cb.Subscribe<CommandStartedEvent>(e => logger.LogTrace("{CommandName} - {Command}",
                                     //                                                       e.CommandName,
                                     //                                                       e.Command.ToJson()));
                                     cb.Subscribe(new DiagnosticsActivityEventSubscriber());
                                   };


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
