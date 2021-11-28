using System;
using System.Threading;

using ArmoniK.Adapters.MongoDB;
using ArmoniK.Compute.gRPC.V1;
using ArmoniK.Core.gRPC;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection;
using ArmoniK.Core.Injection.Options;
using ArmoniK.Core.Storage;

using JetBrains.Annotations;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;
using Microsoft.Extensions.Logging;

using Serilog.Events;
using Serilog;

using GrpcChannel = Grpc.Net.Client.GrpcChannel;


namespace ArmoniK.Compute.PollingAgent
{
  public static class Program
  {
    public static int Main(string[] args)
    {
      Log.Logger = new LoggerConfiguration()
                  .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                  .Enrich.FromLogContext()
                  .WriteTo.Console()
                  .CreateBootstrapLogger();

      try
      {
        Log.Information("Starting host");
        CreateHostBuilder(args).Build().Run();
        return 0;
      }
      catch (Exception ex)
      {
        Log.Fatal(ex, "Host terminated unexpectedly");
        return 1;
      }
      finally
      {
        Log.CloseAndFlush();
      }

    }

    public static IHostBuilder CreateHostBuilder(string[] args)
    {
      var env = new HostingEnvironment();

      return Host.CreateDefaultBuilder(args)
                 .ConfigureHostConfiguration(builder => builder.SetBasePath(env.ContentRootPath)
                                                               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                                               .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                                                               .AddEnvironmentVariables()
                                                               .AddCommandLine(args))
                 .UseSerilog((context, services, config) => config
                                                           .ReadFrom.Configuration(context.Configuration)
                                                           .ReadFrom.Services(services)
                                                           .Enrich.FromLogContext()
                                                           .WriteTo.Console())
                 .ConfigureServices((hostContext, services) =>
                                    {
                                      services.AddLogging()
                                              .AddArmoniKCore(hostContext.Configuration)
                                              .AddMongoComponents(hostContext.Configuration)
                                              .AddHostedService<Worker>()
                                              .AddSingleton<Pollster>()
                                              .AddSingleton<ComputerService.ComputerServiceClient>();
                                    });
    }
  }
}
