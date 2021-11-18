using System;

using Grpc.Net.Client;

using JetBrains.Annotations;

namespace ArmoniK.Compute.PollingAgent
{
  [PublicAPI]
  public record Configuration(string ComputeServiceAddress);

  [PublicAPI]
  internal class Program
  {
    // to load from a teraform generated configuration file
    static Configuration LoadConfiguration() => throw new NotImplementedException();

    static void Main(string[] args)
    {
      var configuration = LoadConfiguration();

      using var channel = GrpcChannel.ForAddress(configuration.ComputeServiceAddress);



    }
  }
}
