using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ArmoniK.Adapters.Factories;
using ArmoniK.Control.Services;
using ArmoniK.Core.Storage;

using Microsoft.Extensions.Logging;
using Serilog.Events;

using Serilog;
using Serilog.Core;

namespace ArmoniK.Control
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddGrpc();
            services.AddSingleton<ILoggerFactory>(sp =>
            {
                Logger serilogLogger = new LoggerConfiguration()
                    .MinimumLevel.Debug()
                    .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                    .Enrich.FromLogContext()
                    .WriteTo.Console()
                    .CreateLogger();
                var loggerFactory = new LoggerFactory();
                loggerFactory.AddSerilog(serilogLogger);
                return loggerFactory;
            });
            services.AddLogging(loggingBuilder => loggingBuilder.AddSerilog(dispose: true));

            services.AddSingleton<ITableStorage>(sp =>
            {
                ILoggerFactory loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                return TableStorageFactory.CreateFromEnv(loggerFactory);
                // TODO : use configuration injection to allow file/commandline configuration
                // see https://docs.microsoft.com/en-us/aspnet/core/fundamentals/configuration/?view=aspnetcore-5.0
                var dbType = Environment.GetEnvironmentVariable("TASKS_STATUS_TABLE_SERVICE");
                if (dbType == null)
                {
                    throw new ArmoniKException("env var TASKS_STATUS_TABLE_SERVICE not found ");
                }

                if (!string.Equals(dbType, "MongoDB"))
                {
                    throw new ArmoniKException("Database type not supported (" + dbType + ")");
                }

                var dbEndpoint = Environment.GetEnvironmentVariable("DB_ENDPOINT");
                if (dbEndpoint == null)
                {
                    throw new ArmoniKException("env var DB_ENDPOINT not found ");
                }

                var dbPort = Environment.GetEnvironmentVariable("DB_PORT");
                if (dbPort == null)
                {
                    throw new ArmoniKException("env var DB_PORT not found ");
                }

                var dbTable = Environment.GetEnvironmentVariable("TASKS_STATUS_TABLE_NAME");
                if (dbTable == null)
                    throw new ArmoniKException("env var TASKS_STATUS_TABLE_NAME not found ");
                }

                //TODO : rework injection to
                // 1. move it to the ArmoniK.Adapters.MongoDB project
                // 2. use all the async elements after the DI is completed (i.e. at runtime)
                //    It will require the creation of different kind of Provider classes
                //    It will allow to reuse the client and the session for all MongoDB classes

                var mongoClient   = new MongoClient("mongodb://" + dbEndpoint + ":" + dbPort);
                var mongoDatabase = mongoClient.GetDatabase(dbTable);
                var clientSessionHandle = mongoClient.StartSession();

                mongoDatabase.CreateCollection(clientSessionHandle,
                                               "session",
                                               new CreateCollectionOptions<SessionDataModel>());
                var sessionCollection = mongoDatabase.GetCollection<SessionDataModel>("session");

                mongoDatabase.CreateCollection(clientSessionHandle,
                                               "task",
                                               new CreateCollectionOptions<TaskDataModel>());
                var taskCollection    = mongoDatabase.GetCollection<TaskDataModel>("task");

                var pollingDelay      = new TimeSpan(seconds: 5, minutes: 0, hours: 0, days: 0);
                var logger            = sp.GetRequiredService<ILogger<TableStorage>>();

                return new TableStorage(sessionCollection, taskCollection,
                                        clientSessionHandle, pollingDelay, logger);
            });
            services.AddSingleton<ClientService, ClientService>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<ClientService>();

                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client. To learn how to create a client, visit: https://go.microsoft.com/fwlink/?linkid=2086909");
                });
            });
        }
    }
}
