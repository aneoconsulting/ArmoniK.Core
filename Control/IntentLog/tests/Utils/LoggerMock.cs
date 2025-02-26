// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using Microsoft.Extensions.Logging;

using Moq;

namespace ArmoniK.Core.Control.IntentLog.Tests.Utils;

public static class LoggerMock
{
  public static void VerifyNoLog<T>(this Mock<ILogger<T>> logger,
                                    LogLevel              logLevel)
    => logger.Verify(m => m.Log(It.Is<LogLevel>(x => x >= logLevel),
                                It.IsAny<EventId>(),
                                It.IsAny<It.IsAnyType>(),
                                It.IsAny<Exception?>(),
                                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                     Times.Never);
}
