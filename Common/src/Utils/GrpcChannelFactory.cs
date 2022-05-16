// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;

using Grpc.Core;
using Grpc.Net.Client;

namespace ArmoniK.Core.Common.Utils;

public static class GrpcChannelFactory
{
  public static GrpcChannel CreateChannel(Options.GrpcClient optionsGrpcClient)
  {
    if (string.IsNullOrEmpty(optionsGrpcClient.Endpoint))
    {
      throw new InvalidOperationException($"{nameof(optionsGrpcClient.Endpoint)} should not be null or empty");
    }
    var uri = new Uri(optionsGrpcClient.Endpoint);

    var credentials = uri.Scheme == Uri.UriSchemeHttps
                        ? new SslCredentials()
                        : ChannelCredentials.Insecure;
    HttpClientHandler httpClientHandler = new HttpClientHandler();

    if (!optionsGrpcClient.SslValidation)
    {
      httpClientHandler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
      AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport",
                           true);
    }

    if (!string.IsNullOrEmpty(optionsGrpcClient.CertPem) && string.IsNullOrEmpty(optionsGrpcClient.KeyPem))
    {
      var cert = X509Certificate2.CreateFromPemFile(optionsGrpcClient.CertPem,
                                                    optionsGrpcClient.KeyPem);

      // Resolve issue with Windows on pem bug with windows
      // https://github.com/dotnet/runtime/issues/23749#issuecomment-388231655
      if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
      {
        var originalCert = cert;
        cert = new X509Certificate2(cert.Export(X509ContentType.Pkcs12));
        originalCert.Dispose();
      }

      httpClientHandler.ClientCertificates.Add(cert);
    }

    var channelOptions = new GrpcChannelOptions()
                         {
                           Credentials = credentials,
                           HttpHandler = httpClientHandler,
                         };

    var channel = GrpcChannel.ForAddress(optionsGrpcClient.Endpoint,
                                         channelOptions);

    return channel;
  }
}
