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
using System.IO;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
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
      logger.LogInformation("Starting X509 certificate configuration.");

      if (!File.Exists(mongoOptions.CAFile))
      {
        throw new FileNotFoundException("CA certificate file not found",
                                        mongoOptions.CAFile);
      }

      logger.LogInformation("CA file found at path: {path}",
                            mongoOptions.CAFile);
      logger.LogInformation("Raw CA file path: {path}",
                            mongoOptions.CAFile);

      // Load the CA certificate
      try
      {
        var authority = new X509Certificate2(mongoOptions.CAFile);
        logger.LogInformation("CA certificate loaded: {authority.Subject}",
                              authority.Subject);

        //  SSL Parameters configuration
        settings.SslSettings = new SslSettings
                               {
                                 ClientCertificates  = new X509Certificate2Collection(authority),
                                 EnabledSslProtocols = SslProtocols.Tls12,
                                 ServerCertificateValidationCallback = (sender,
                                                                        certificate,
                                                                        certChain,
                                                                        sslPolicyErrors) =>
                                                                       {
                                                                         if (sslPolicyErrors == SslPolicyErrors.None)
                                                                         {
                                                                           logger.LogInformation("SSL validation successful: no errors.");
                                                                           return true;
                                                                         }

                                                                         logger.LogError("SSL validation failed with errors: {sslPolicyErrors}",
                                                                                         sslPolicyErrors);

                                                                         if (certificate == null)
                                                                         {
                                                                           logger.LogError("Certificate is null!");
                                                                           return false;
                                                                         }

                                                                         var cert = new X509Certificate2(certificate);
                                                                         if (certChain == null)
                                                                         {
                                                                           logger.LogError("Certificate chain is null!");
                                                                           return false;
                                                                         }

                                                                         certChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                                                                         certChain.ChainPolicy.VerificationFlags =
                                                                           X509VerificationFlags.AllowUnknownCertificateAuthority;
                                                                         // Build the chain 
                                                                         if (!certChain.Build(cert))
                                                                         {
                                                                           logger.LogError("SSL chain validation failed.");
                                                                           foreach (var status in certChain.ChainStatus)
                                                                           {
                                                                             logger.LogError("ChainStatus: {status.StatusInformation} ({status.Status})",
                                                                                             status.StatusInformation,
                                                                                             status.Status);
                                                                           }

                                                                           return false;
                                                                         }

                                                                         // Verification of the chain root 
                                                                         if (authority != null)
                                                                         {
                                                                           certChain.ChainPolicy.ExtraStore.Add(authority);
                                                                           logger.LogInformation("Added CA certificate to chain policy.");
                                                                           var isTrusted =
                                                                             certChain.ChainElements.Any(x => x.Certificate.Thumbprint == authority.Thumbprint);

                                                                           if (!isTrusted)
                                                                           {
                                                                             logger.LogError("Certificate chain root does not match the specified CA authority.");
                                                                             return false;
                                                                           }
                                                                         }


                                                                         var validHosts = new[]
                                                                                          {
                                                                                            mongoOptions.Host,
                                                                                            "127.0.0.1",
                                                                                            "localhost"
                                                                                          };
                                                                         if (!validHosts.Any(host => cert.Subject.Contains($"CN={host}",
                                                                                                                           StringComparison.OrdinalIgnoreCase)))
                                                                         {
                                                                           logger
                                                                             .LogError("Certificate host mismatch. Expected one of: {ValidHosts}, but found: {CertSubject}",
                                                                                       validHosts,
                                                                                       cert.Subject);
                                                                           return false;
                                                                         }

                                                                         logger.LogInformation("SSL validation successful.");
                                                                         return true;
                                                                       }
                               };
      }
      catch (CryptographicException e)
      {
        logger.LogError(e,
                        "Error loading CA certificate: {message}",
                        e.Message);
        logger.LogError(e.InnerException,
                        "Inner exception: {message}",
                        e.InnerException?.Message);
        logger.LogError("Stack trace: {stackTrace}",
                        e.StackTrace);
        logger.LogError("Help link: {helpLink}",
                        e.HelpLink);

        logger.LogError("HResult: {hResult}",
                        e.HResult);
        logger.LogError("Source: {source}",
                        e.Source);
        logger.LogError("Exception Data: {Data}",
                        e.Data);
        throw;
      }
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
