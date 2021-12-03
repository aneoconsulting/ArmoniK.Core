using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

using Serilog;
using Serilog.Events;

using System;

namespace ArmoniK.Control
{
  public class Program
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
        Log.Information("Starting web host");
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

    // Additional configuration is required to successfully run gRPC on macOS.
    // For instructions on how to configure Kestrel and gRPC clients on macOS, visit https://go.microsoft.com/fwlink/?linkid=2099682
    public static IHostBuilder CreateHostBuilder(string[] args) =>
      Host.CreateDefaultBuilder(args)
          .UseSerilog((context, services, configuration) => configuration
                                                            .ReadFrom.Configuration(context.Configuration)
                                                            .ReadFrom.Services(services)
                                                            .MinimumLevel
                                                            .Override("Microsoft.AspNetCore", LogEventLevel.Debug)
                                                            .Enrich.FromLogContext())
          .ConfigureWebHostDefaults(webBuilder => { webBuilder.UseStartup<Startup>(); });
  }
}