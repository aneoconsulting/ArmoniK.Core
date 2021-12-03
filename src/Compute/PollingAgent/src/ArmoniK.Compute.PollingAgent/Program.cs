using System;
using System.IO;

using ArmoniK.Adapters.MongoDB;
using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.Injection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.Internal;

using Serilog.Events;
using Serilog;


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
      var env = new HostingEnvironment()
      {
          ContentRootPath = Directory.GetCurrentDirectory()
      };
      return Host.CreateDefaultBuilder(args)
                 .ConfigureHostConfiguration(builder => builder.SetBasePath(env.ContentRootPath)
                                                               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                                               .AddJsonFile($"appsettings.{env.EnvironmentName}.json", optional: true)
                                                               .AddEnvironmentVariables()
                                                               .AddCommandLine(args))
                 .UseSerilog((context, services, config) => config
                                                           .ReadFrom.Configuration(context.Configuration)
                                                           .ReadFrom.Services(services)
                                                           .Enrich.FromLogContext())
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
