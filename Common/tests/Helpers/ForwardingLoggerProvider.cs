// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
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

using System;

using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public delegate void LogMessage(LogLevel   logLevel,
                                string     categoryName,
                                EventId    eventId,
                                string     message,
                                Exception? exception);

internal class ForwardingLoggerProvider : ILoggerProvider
{
  private readonly LogMessage logAction_;

  public ForwardingLoggerProvider(LogMessage logAction)
    => logAction_ = logAction;

  public ILogger CreateLogger(string categoryName)
    => new ForwardingLogger(categoryName,
                            logAction_);

  public void Dispose()
  {
  }

  internal class ForwardingLogger : ILogger
  {
    private readonly string     categoryName_;
    private readonly LogMessage logAction_;

    public ForwardingLogger(string     categoryName,
                            LogMessage logAction)
    {
      categoryName_ = categoryName;
      logAction_    = logAction;
    }

    public bool IsEnabled(LogLevel logLevel)
      => true;

    public IDisposable BeginScope<TState>(TState state)
      where TState : notnull
      => Disposable.Create(() =>
                           {
                           });

    public void Log<TState>(LogLevel                         logLevel,
                            EventId                          eventId,
                            TState                           state,
                            Exception?                       exception,
                            Func<TState, Exception?, string> formatter)
      => logAction_(logLevel,
                    categoryName_,
                    eventId,
                    formatter(state,
                              exception),
                    exception);
  }
}
