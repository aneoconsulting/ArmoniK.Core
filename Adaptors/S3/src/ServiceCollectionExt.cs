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

using Amazon.S3;

using ArmoniK.Api.Common.Utils;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Utils;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Adapters.S3;

public static class ServiceCollectionExt
{
  [PublicAPI]
  public static IServiceCollection AddS3(this IServiceCollection serviceCollection,
                                         ConfigurationManager    configuration,
                                         ILogger                 logger)
  {
    var components = configuration.GetSection(Components.SettingSection);

    if (components["ObjectStorage"] == "ArmoniK.Adapters.S3.ObjectStorage")
    {
      // ReSharper disable once InlineOutVariableDeclaration
      Options.S3 s3Options;
      serviceCollection.AddOption(configuration,
                                  Options.S3.SettingSection,
                                  out s3Options);

      using var _ = logger.BeginNamedScope("S3 configuration",
                                           ("EndpointUrl", s3Options.EndpointUrl));

      logger.LogInformation("setup connection to S3 at {EndpointUrl} with user {user} with option ForcePathStyle = {ForcePathStyle} with BucketName = {BucketName}",
                            s3Options.EndpointUrl,
                            s3Options.Login,
                            s3Options.MustForcePathStyle,
                            s3Options.BucketName);

      var s3Config = new AmazonS3Config
                     {
                       ForcePathStyle = s3Options.MustForcePathStyle,
                       ServiceURL     = s3Options.EndpointUrl,
                     };

      AmazonS3Client s3Client;
      if (string.IsNullOrWhiteSpace(s3Options.Login))
      {
        s3Client = new AmazonS3Client(s3Config);
      }
      else
      {
        s3Client = new AmazonS3Client(s3Options.Login,
                                      s3Options.Password,
                                      s3Config);
      }


      serviceCollection.AddSingleton(_ => s3Client);
      serviceCollection.AddSingletonWithHealthCheck<IObjectStorage, ObjectStorage>(nameof(IObjectStorage));
    }

    return serviceCollection;
  }
}
