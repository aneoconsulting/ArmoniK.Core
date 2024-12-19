// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Diagnostics;

using Destructurama;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

using Serilog;
using Serilog.Core;
using Serilog.Formatting.Compact;

using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace ArmoniK.Core.Common.Utils;

public class LoggerInit
{
  private readonly ILogger logger_;
  private readonly Logger  loggerConfiguration_;

  public LoggerInit(IConfiguration configuration)
  {
    var instanceId = Guid.NewGuid()
                         .ToString();
    loggerConfiguration_ = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                                    .WriteTo.Console(new CompactJsonFormatter())
                                                    .Enrich.FromLogContext()
                                                    .Enrich.WithProperty("CoreVersion",
                                                                         FileVersionInfo.GetVersionInfo(typeof(LoggerInit).Assembly.Location)
                                                                                        .ProductVersion ?? "Unknown")
                                                    .Enrich.WithProperty("InstanceId",
                                                                         instanceId)
                                                    .Destructure.UsingAttributes()
                                                    .CreateLogger();

    logger_ = LoggerFactory.Create(builder => builder.AddSerilog(loggerConfiguration_))
                           .CreateLogger("root");
  }

  public void Configure(ILoggingBuilder loggingBuilder)
    => loggingBuilder.AddSerilog(loggerConfiguration_);

  public Logger GetSerilogConf()
    => loggerConfiguration_;

  public ILogger GetLogger()
    => logger_;
}
