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

using System;
using System.Collections.Generic;

using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

public delegate void LogMessage(LogLevel                                logLevel,
                                string                                  categoryName,
                                EventId                                 eventId,
                                string                                  message,
                                ICollection<Dictionary<string, object>> states,
                                Exception?                              exception);

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
    private readonly string                           categoryName_;
    private readonly LogMessage                       logAction_;
    private readonly List<Dictionary<string, object>> states_;

    public ForwardingLogger(string     categoryName,
                            LogMessage logAction)
    {
      categoryName_ = categoryName;
      logAction_    = logAction;
      states_       = new List<Dictionary<string, object>>();
    }

    public bool IsEnabled(LogLevel logLevel)
      => true;

    public IDisposable BeginScope<TState>(TState state)
      where TState : notnull
    {
      if (state is not Dictionary<string, object> d)
      {
        return new Deferrer(() =>
                            {
                            });
      }

      states_.Add(d);
      return new Deferrer(() =>
                          {
                            states_.Remove(d);
                          });
    }

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
                    states_,
                    exception);
  }
}
