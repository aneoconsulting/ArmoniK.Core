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

using ArmoniK.Core.Base.Exceptions;

namespace ArmoniK.Core.Common.Exceptions;

/// <summary>
///   Exception that is thrown when a timeout occurs.
/// </summary>
[Serializable]
public class TimeoutException : ArmoniKException
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="TimeoutException" /> class.
  /// </summary>
  public TimeoutException()
  {
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="TimeoutException" /> class with a specified error message.
  /// </summary>
  /// <param name="message">The message that describes the error.</param>
  public TimeoutException(string message)
    : base(message)
  {
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="TimeoutException" /> class with a specified error message and a
  ///   reference to the inner exception that is the cause of this exception.
  /// </summary>
  /// <param name="message">The message that describes the error.</param>
  /// <param name="innerException">The exception that is the cause of the current exception.</param>
  public TimeoutException(string    message,
                          Exception innerException)
    : base(message,
           innerException)
  {
  }
}
