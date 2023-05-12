// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Common.Utils;
using ArmoniK.Core.Utils;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(RpcExt))]
public class LoggerExtTest
{
  [Test]
  public void LogVersionShouldSucceed()
  {
    var loggerProvider = new ConsoleForwardingLoggerProvider();
    var logger         = loggerProvider.CreateLogger("root");

    logger.LogVersion(typeof(LoggerExtTest));
    logger.LogVersion(typeof(gRPC.Services.Submitter));

    Assert.Pass();
  }

  [Test]
  public void LoggerInitShouldSucceed()
  {
    var conf       = new ConfigurationManager();
    var loggerInit = new LoggerInit(conf);

    var logger = loggerInit.GetLogger();

    Assert.IsNotNull(logger);

    logger.LogVersion(typeof(LoggerExtTest));
  }
}
