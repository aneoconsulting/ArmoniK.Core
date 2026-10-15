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

namespace ArmoniK.Core.Common.Exceptions;

/// <summary>
///   A version of a permission set is stored with other permissions: a stored version is never rewritten, since audit
///   records cite permission sets by version
/// </summary>
[Serializable]
public class PermissionSetVersionAlreadyExistsException : ArmoniKException
{
  /// <summary>
  ///   Build the exception
  /// </summary>
  public PermissionSetVersionAlreadyExistsException()
  {
  }

  /// <summary>
  ///   Build the exception
  /// </summary>
  /// <param name="message">The version and its stored content</param>
  public PermissionSetVersionAlreadyExistsException(string message)
    : base(message)
  {
  }

  /// <summary>
  ///   Build the exception
  /// </summary>
  /// <param name="message">The version and its stored content</param>
  /// <param name="innerException">The cause</param>
  public PermissionSetVersionAlreadyExistsException(string    message,
                                    Exception innerException)
    : base(message,
           innerException)
  {
  }
}
