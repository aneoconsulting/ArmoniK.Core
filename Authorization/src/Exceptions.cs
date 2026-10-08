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

using ArmoniK.Core.Base.Exceptions;

namespace ArmoniK.Core.Authorization;

/// <summary>
///   Why a grant is refused when it is written
/// </summary>
public enum InvalidGrantKind
{
  /// <summary>The condition does not parse, or the permission set does not exist</summary>
  Malformed,

  /// <summary>An identifier lacks the prefix that says what it is</summary>
  MissingPrefix,

  /// <summary>No permission of the set could ever be reached under the condition</summary>
  Unreachable,

  /// <summary>The grant would reach far more than it names</summary>
  Overreaching,
}

/// <summary>
///   Exception thrown when a grant is refused
/// </summary>
[Serializable]
public class InvalidGrantException : ArmoniKException
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="InvalidGrantException" /> class
  /// </summary>
  /// <param name="kind">Why the grant is refused</param>
  /// <param name="message">The message that describes the error</param>
  /// <param name="innerException">The exception that is the cause of this exception</param>
  public InvalidGrantException(InvalidGrantKind kind,
                               string           message,
                               Exception?       innerException = null)
    : base(message,
           innerException)
    => Kind = kind;

  /// <summary>Why the grant is refused</summary>
  public InvalidGrantKind Kind { get; }
}

/// <summary>
///   Exception thrown when a condition does not belong to the condition grammar
/// </summary>
[Serializable]
public class InvalidConditionException : ArmoniKException
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="InvalidConditionException" /> class
  /// </summary>
  /// <param name="message">The message that describes the error</param>
  public InvalidConditionException(string message)
    : base(message)
  {
  }
}

/// <summary>
///   Exception thrown when a permission set is refused
/// </summary>
[Serializable]
public class InvalidPermissionSetException : ArmoniKException
{
  /// <summary>
  ///   Initializes a new instance of the <see cref="InvalidPermissionSetException" /> class
  /// </summary>
  /// <param name="message">The message that describes the error</param>
  public InvalidPermissionSetException(string message)
    : base(message)
  {
  }
}
