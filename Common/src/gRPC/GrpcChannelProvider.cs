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
using System.Net.Sockets;
using System.Threading.Tasks;
using System.IO;
using Mono.Unix;

using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Injection.Options;

using Grpc.Core;
using Grpc.Net.Client;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using GrpcChannel = ArmoniK.Core.Common.Injection.Options.GrpcChannel;


namespace ArmoniK.Core.Common.gRPC;

[UsedImplicitly]
public class GrpcChannelProvider : ProviderBase<ChannelBase>
{
    private readonly bool isInitialized_;

    // ReSharper disable once SuggestBaseTypeForParameterInConstructor
    public GrpcChannelProvider(GrpcChannel options,
                               ILogger<GrpcChannelProvider> logger)
      : base(options.SocketType == GrpcSocketType.Web
               ? () => Task.FromResult(BuildWebGrpcChannel(options.Address ?? throw new InvalidOperationException(),
                                                           logger))
               : () => Task.FromResult(BuildUnixSocketGrpcChannel(options.Address ?? throw new InvalidOperationException(),
                                                                  logger)))
      => isInitialized_ = true;

    private static ChannelBase BuildWebGrpcChannel(string address,
                                                   ILogger logger)
    {
        using var _ = logger.LogFunction();
        return Grpc.Net.Client.GrpcChannel.ForAddress(address);
    }

    private static ChannelBase BuildUnixSocketGrpcChannel(string address,
                                                          ILogger logger)
    {
        using var _ = logger.LogFunction();

        var udsEndPoint = new UnixDomainSocketEndPoint(address);

        var socketsHttpHandler = new SocketsHttpHandler
        {
            ConnectCallback = async (unknown,
                                     cancellationToken) =>
                              {
                                  var socket = new Socket(AddressFamily.Unix,
                                                                             SocketType.Stream,
                                                                             ProtocolType.Unspecified);

                                  try
                                  {
                                      await socket.ConnectAsync(udsEndPoint,
                                                                                   cancellationToken)
                                                                     .ConfigureAwait(false);
                                      return new NetworkStream(socket,
                                                                                  true);
                                  }
                                  catch
                                  {
                                      socket.Dispose();
                                      throw;
                                  }
                              },
        };
        var fInfo = new UnixFileInfo(address)
        {
            FileAccessPermissions = FileAccessPermissions.AllPermissions
        };
        fInfo.Refresh();
        return Grpc.Net.Client.GrpcChannel.ForAddress("http://localhost",
                                                    new GrpcChannelOptions
                                                    {
                                                        HttpHandler = socketsHttpHandler,
                                                    });
    }

    public override ValueTask<bool> Check(HealthCheckTag tag)
      => ValueTask.FromResult(isInitialized_);
}
