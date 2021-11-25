using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Control.Services;
using ArmoniK.Core.Storage;
using ArmoniK.Adapters.MongoDB;
using ArmoniK.Core.Exceptions;
using MongoDB.Driver;

namespace ArmoniK.Control
{
    public class Startup
    {
        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddGrpc();
            services.AddSingleton<ITableStorage>(sp =>
                {
                    string db_type = Environment.GetEnvironmentVariable("TASKS_STATUS_TABLE_SERVICE");
                    if (db_type == null)
                    {
                        throw new ArmoniKException("env var TASKS_STATUS_TABLE_SERVICE not found ");
                    }
                    if (!string.Equals(db_type, "MongoDB"))
                    {
                        throw new ArmoniKException("Database type not supported (" + db_type + ")");
                    }
                    string db_endpoint = Environment.GetEnvironmentVariable("DB_ENDPOINT");
                    if (db_endpoint == null)
                    {
                        throw new ArmoniKException("env var DB_ENDPOINT not found ");
                    }
                    string db_port = Environment.GetEnvironmentVariable("DB_PORT");
                    if (db_port == null)
                    {
                        throw new ArmoniKException("env var DB_PORT not found ");
                    }
                    string db_table = Environment.GetEnvironmentVariable("TASKS_STATUS_TABLE_NAME");
                    if (db_table == null)
                    {
                        throw new ArmoniKException("env var TASKS_STATUS_TABLE_NAME not found ");
                    }
                    MongoClient mongoClient = new MongoClient("mongodb://" + db_endpoint + ":" + db_port);
                    IMongoDatabase mongoDatabase = mongoClient.GetDatabase(db_table);
                    mongoDatabase.CreateCollection("session", new CreateCollectionOptions<SessionDataModel>());
                    mongoDatabase.CreateCollection("task", new CreateCollectionOptions<TaskDataModel>());
                    IMongoCollection<SessionDataModel> sessionCollection = mongoDatabase.GetCollection<SessionDataModel>("session");
                    IMongoCollection<TaskDataModel> taskCollection = mongoDatabase.GetCollection<TaskDataModel>("task");
                    TimeSpan pollingDelay = new TimeSpan(seconds: 5, minutes: 0, hours: 0, days: 0);
                    return new TableStorage(sessionCollection, taskCollection, mongoClient.StartSession(), pollingDelay);
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
