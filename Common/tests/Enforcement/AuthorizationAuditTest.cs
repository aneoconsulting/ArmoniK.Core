// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using ArmoniK.Core.Common.Enforcement;

using Microsoft.Extensions.Logging;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Enforcement;

[TestFixture(TestOf = typeof(AuthorizationAudit))]
public class AuthorizationAuditTest
{
  [Test]
  public void RecordsAreWrittenUnderTheCategoryPublishedToOperators()
  {
    var logger = new Mock<ILogger>();
    logger.Setup(l => l.IsEnabled(It.IsAny<LogLevel>()))
          .Returns(true);
    var factory = new Mock<ILoggerFactory>();
    factory.Setup(f => f.CreateLogger(It.IsAny<string>()))
           .Returns(logger.Object);

    var audit = new AuthorizationAudit(factory.Object);
    audit.RecordFault("permission set team-reader",
                      "permissions that are not in the catalogue are left out: session:archive");

    // Written literally: operators filter on this name, so it must not follow a rename of the class
    factory.Verify(f => f.CreateLogger("ArmoniK.Authorization.Audit"),
                   Times.Once);
    factory.Verify(f => f.CreateLogger(It.Is<string>(category => category != "ArmoniK.Authorization.Audit")),
                   Times.Never);
    logger.Verify(l => l.Log(It.IsAny<LogLevel>(),
                             It.IsAny<EventId>(),
                             It.IsAny<It.IsAnyType>(),
                             It.IsAny<Exception?>(),
                             It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                  Times.Once);
  }
}
