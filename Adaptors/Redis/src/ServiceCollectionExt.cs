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
using System.IO;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.StackExchangeRedis;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using StackExchange.Redis;

namespace ArmoniK.Core.Adapters.Redis
{
  public static class ServiceCollectionExt
  {
    [PublicAPI]
    public static IServiceCollection AddRedis(
      this IServiceCollection serviceCollection,
      IConfiguration          configuration,
      ILogger                 logger)
    {
      var redisOptions = configuration.GetSection(Options.Redis.SettingSection).Get<Options.Redis>();

      serviceCollection.AddStackExchangeRedisCache(options =>
      {
        options.ConfigurationOptions = new()
        {
          ClientName = redisOptions.ClientName,
          ReconnectRetryPolicy = new ExponentialRetry(10),
          Ssl                  = true,
          EndPoints =
          {
            redisOptions.EndpointUrl,
          },
          SslHost            = redisOptions.SslHost,
          AbortOnConnectFail = true,
          ConnectTimeout     = redisOptions.Timeout,
          //CheckCertificateRevocation = xx,
          //SslProtocols               = xx,
        };
        logger.LogDebug("setup connection to Redis at {EndpointUrl}", redisOptions.EndpointUrl);

        if (!File.Exists(redisOptions.CaCertPath))
        {
          logger.LogError(redisOptions.CaCertPath + " was not found !");
          throw new FileNotFoundException(redisOptions.CaCertPath + " was not found !");
        }

        if (!File.Exists(redisOptions.ClientPfxPath))
        {
          logger.LogError(redisOptions.ClientPfxPath + " was not found !");
          throw new FileNotFoundException(redisOptions.ClientPfxPath + " was not found !");
        }

        options.ConfigurationOptions.CertificateValidation += (sender, certificate, chain, sslPolicyErrors) =>
        {
          X509Certificate2 certificateAuthority = new(redisOptions.CaCertPath);
          if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
          {
            var root = chain.ChainElements[^1].Certificate;
            return certificateAuthority.Equals(root);
          }

          return sslPolicyErrors == SslPolicyErrors.None;
        };

        options.ConfigurationOptions.CertificateSelection += delegate
        {
          var cert = new X509Certificate2(redisOptions.ClientPfxPath);
          return cert;
        };

        //options.InstanceName = redisOptions.InstanceName;
      });

      var components = configuration.GetSection(Components.SettingSection);

      if (components["ObjectStorage"] == "ArmoniK.Adapters.Redis.ObjectStorage")
      {
        serviceCollection.AddSingleton<IDistributedCache, RedisCache>();
        serviceCollection.AddSingleton<IObjectStorage, DistributedCacheObjectStorage>();
      }

      return serviceCollection;
    }
  }
}