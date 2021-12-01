// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Injection;
using ArmoniK.Core.Injection.Options;

using Grpc.Core;

using Microsoft.Extensions.Options;

using GrpcChannel = Grpc.Net.Client.GrpcChannel;
using Grpc.Net.Client;

namespace ArmoniK.Core.gRPC
{

  public class GrpcChannelProvider : ProviderBase<ChannelBase>
  {
    public GrpcChannelProvider(IOptions<Injection.Options.GrpcChannel> options)
      : base(options.Value.SocketType == GrpcSocketType.Web
               ? () => Task.FromResult(BuildWebGrpcChannel(options.Value.Address))
               : () => Task.FromResult(BuildUnixSocketGrpcChannel(options.Value.Address)))
    {
    }

    private static ChannelBase BuildWebGrpcChannel(string address) => GrpcChannel.ForAddress(address);

    private static ChannelBase BuildUnixSocketGrpcChannel(string address)
    {
      var udsEndPoint = new UnixDomainSocketEndPoint(address);

      var socketsHttpHandler = new SocketsHttpHandler
                               {
                                 ConnectCallback = async (unknown, cancellationToken) =>
                                                   {
                                                     var socket = new Socket(AddressFamily.Unix, SocketType.Stream,
                                                                             ProtocolType.Unspecified);

                                                     try
                                                     {
                                                       await socket.ConnectAsync(udsEndPoint, cancellationToken).ConfigureAwait(false);
                                                       return new NetworkStream(socket, true);
                                                     }
                                                     catch
                                                     {
                                                       socket.Dispose();
                                                       throw;
                                                     }
                                                   },
                               };

      return GrpcChannel.ForAddress("http://localhost", new GrpcChannelOptions
                                                        {
                                                          HttpHandler = socketsHttpHandler,
                                                        });
    }
  }
}
