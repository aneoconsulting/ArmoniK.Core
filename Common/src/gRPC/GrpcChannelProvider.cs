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

using ArmoniK.Core.Common.Injection.Options;

using Grpc.Core;
using Grpc.Net.Client;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using GrpcChannel = ArmoniK.Core.Common.Injection.Options.GrpcChannel;

namespace ArmoniK.Core.Common.gRPC;

[UsedImplicitly]
public class GrpcChannelProvider : IAsyncDisposable
{
  private readonly GrpcChannel                  options_;
  private readonly ILogger<GrpcChannelProvider> logger_;
  private readonly string                       address_;
  private          Socket?                      socket_;
  private          NetworkStream?               networkStream_;

  public GrpcChannelProvider(GrpcChannel                  options,
                             ILogger<GrpcChannelProvider> logger)
  {
    options_ = options;
    logger_  = logger;
    address_ = options_.Address ?? throw new InvalidOperationException();
    logger.LogDebug("Channel created for address : {address}",
                    address_);
  }

  private static ChannelBase BuildWebGrpcChannel(string  address,
                                                 ILogger logger)
  {
    using var _ = logger.LogFunction();
    return Grpc.Net.Client.GrpcChannel.ForAddress(address);
  }

  private ChannelBase BuildUnixSocketGrpcChannel(string  address,
                                                 ILogger logger)
  {
    using var _ = logger.LogFunction();

    var udsEndPoint = new UnixDomainSocketEndPoint(address);

    var socketsHttpHandler = new SocketsHttpHandler
                             {
                               ConnectCallback = async (_,
                                                        cancellationToken) =>
                                                 {
                                                   socket_ = new Socket(AddressFamily.Unix,
                                                                        SocketType.Stream,
                                                                        ProtocolType.Unspecified);

                                                   try
                                                   {
                                                     await socket_.ConnectAsync(udsEndPoint,
                                                                                cancellationToken)
                                                                  .ConfigureAwait(false);
                                                     networkStream_ = new NetworkStream(socket_,
                                                                                        true);
                                                     return networkStream_;
                                                   }
                                                   catch
                                                   {
                                                     socket_.Dispose();
                                                     throw;
                                                   }
                                                 },
                             };

    return Grpc.Net.Client.GrpcChannel.ForAddress("http://localhost",
                                                  new GrpcChannelOptions
                                                  {
                                                    HttpHandler = socketsHttpHandler,
                                                  });
  }

  public ChannelBase Get()
  {
    switch (options_.SocketType)
    {
      case GrpcSocketType.Web:
        return BuildWebGrpcChannel(address_,
                                   logger_);
      case GrpcSocketType.UnixSocket:
        return BuildUnixSocketGrpcChannel(address_,
                                          logger_);
      default:
        throw new InvalidOperationException();
    }
  }

  public async ValueTask DisposeAsync()
  {
    socket_?.Close();
    socket_?.Dispose();
    if (networkStream_ != null)
    {
      await networkStream_.DisposeAsync()
                          .ConfigureAwait(false);
    }
  }
}
