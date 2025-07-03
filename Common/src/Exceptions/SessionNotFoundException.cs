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
///   Exception thrown when a session is not found.
/// </summary>
[Serializable]
public class SessionNotFoundException : ArmoniKException
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionNotFoundException" /> class.
  /// </summary>
  public SessionNotFoundException()
  {
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionNotFoundException" /> class.
  /// </summary>
  /// <param name="deleted">Indicates whether the session was deleted.</param>
  public SessionNotFoundException(bool deleted)
    => Deleted = deleted;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionNotFoundException" /> class with a specified error message.
  /// </summary>
  /// <param name="message">The message that describes the error.</param>
  /// <param name="deleted">Indicates whether the session was deleted.</param>
  public SessionNotFoundException(string message,
                                  bool   deleted = false)
    : base(message)
    => Deleted = deleted;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionNotFoundException" /> class with a specified error message and a
  ///   reference to the inner exception that is the cause of this exception.
  /// </summary>
  /// <param name="message">The error message that explains the reason for the exception.</param>
  /// <param name="innerException">
  ///   The exception that is the cause of the current exception, or a null reference if no inner
  ///   exception is specified.
  /// </param>
  /// <param name="deleted">Indicates whether the session was deleted.</param>
  public SessionNotFoundException(string    message,
                                  Exception innerException,
                                  bool      deleted = false)
    : base(message,
           innerException)
    => Deleted = deleted;

  /// <summary>
  ///   Gets a value indicating whether the session was deleted.
  /// </summary>
  public bool Deleted { get; }
}
