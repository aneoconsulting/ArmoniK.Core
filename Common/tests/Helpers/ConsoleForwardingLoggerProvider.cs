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
using System.Linq;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Tests.Helpers;

internal class ConsoleForwardingLoggerProvider : ILoggerProvider
{
  private readonly ForwardingLoggerProvider provider_;

  public ConsoleForwardingLoggerProvider(LogLevel minLogLevel)
    => provider_ = new ForwardingLoggerProvider((logLevel,
                                                 category,
                                                 _,
                                                 message,
                                                 states,
                                                 exception) =>
                                                {
                                                  if (logLevel >= minLogLevel)
                                                  {
                                                    var keyValuePairs = states.SelectMany(objects => objects.AsEnumerable())
                                                                              .Select(pair => $"{pair.Key}:{pair.Value}");
                                                    var str = string.Join(", ",
                                                                          keyValuePairs);
                                                    Console.WriteLine($"{logLevel} : {DateTime.Now}  =>  {category} \n Properties : {str} \n {message} \n {exception}");
                                                  }
                                                });

  public ConsoleForwardingLoggerProvider()
    : this(LogLevel.Trace)
  {
  }

  public void Dispose()
    => provider_.Dispose();

  public ILogger CreateLogger(string categoryName)
    => provider_.CreateLogger(categoryName);
}
