// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using ArmoniK.Adapters.Amqp;
using ArmoniK.Adapters.MongoDB;
using ArmoniK.Control.Services;
using ArmoniK.Core.Injection;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using Serilog;

namespace ArmoniK.Control
{
  public class Startup
  {
    public Startup(IWebHostEnvironment env)
    {
      var builder = new ConfigurationBuilder()
                   .SetBasePath(env.ContentRootPath)
                   .AddJsonFile("appsettings.json",
                                true,
                                true)
                   .AddJsonFile($"appsettings.{env.EnvironmentName}.json",
                                true)
                   .AddEnvironmentVariables();


      Configuration = builder.Build();
    }

    public IConfiguration Configuration { get; }

    // This method gets called by the runtime. Use this method to add services to the container.
    // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
    public void ConfigureServices(IServiceCollection services)
    {
      services.AddMongoComponents(Configuration);
      services.AddAmqp(Configuration);
      services.ValidateGrpcRequests();
    }

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
    {
      if (env.IsDevelopment())
        app.UseDeveloperExceptionPage();

      app.UseSerilogRequestLogging();

      app.UseRouting();

      app.UseEndpoints(endpoints =>
                       {
                         endpoints.MapGrpcService<ClientService>();
                       });
    }
  }
}
