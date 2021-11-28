using System;
using System.Threading;

using ArmoniK.Compute.gRPC.V1;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Storage;

using Grpc.Net.Client;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using Serilog.Events;
using Serilog;


namespace ArmoniK.Compute.PollingAgent
{
  //TODO : migrate to the worker template instead of the console app one
  [PublicAPI]
  internal class Program
  {

    static void Main()
    {
      // TODO: use interprocess communication (see https://docs.microsoft.com/en-us/aspnet/core/grpc/interprocess?view=aspnetcore-5.0)

      var configuration = Configuration.LoadFromFile("filename");

      using var channel = GrpcChannel.ForAddress(configuration.ComputeServiceAddress);

      var serilogLogger = new LoggerConfiguration()
                         .MinimumLevel.Debug()
                         .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                         .Enrich.FromLogContext()
                         .WriteTo.Console()
                         .CreateLogger();

      var loggerFactory = new LoggerFactory();
      loggerFactory.AddSerilog(serilogLogger);
      var logger = loggerFactory.CreateLogger<Program>();

      ITableStorage tableStorage = null;
      IQueueStorage queueStorage = null;
      ILeaseProvider leaseProvider = null;
      IObjectStorage objectStorage = null;
      var taskResultStorage = new KeyValueStorage<TaskId, ComputeReply>("TaskResult", objectStorage);
      var taskPayloadStorage = new KeyValueStorage<TaskId, Payload>("TaskPayload", objectStorage);
      var client = new ComputerService.ComputerServiceClient(channel);

      var pollster = new Pollster(loggerFactory.CreateLogger<Pollster>(), 
                                 1, 
                                 queueStorage, 
                                 tableStorage, 
                                 leaseProvider,
                                 taskResultStorage, 
                                 taskPayloadStorage, 
                                 client);

      var cts            = new CancellationTokenSource();

      var sigintReceived = false;

      Console.CancelKeyPress += (_, ea) =>
                                {
                                  // Tell .NET to not terminate the process
                                  ea.Cancel = true;
                                  logger.LogCritical("Received SIGINT (Ctrl+C)");
                                  cts.Cancel();
                                  sigintReceived = true;
                                };

      AppDomain.CurrentDomain.ProcessExit += (_, _) =>
                                             {
                                               if (!sigintReceived)
                                               {
                                                 logger.LogCritical("Received SIGTERM");
                                                 cts.Cancel();
                                               }
                                               else
                                               {
                                                 logger.LogCritical("Received SIGTERM, ignoring it because already processed SIGINT");
                                               }
                                             };


      pollster.MainLoop(cts.Token).Wait(cts.Token);

    }
  }
}
