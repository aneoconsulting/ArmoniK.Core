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
///   The grants in force could not be read, so that requests cannot be decided: answered as unavailable, without
///   counting toward the errors that stop the control plane, since a failed read is counted once where it happens
/// </summary>
[Serializable]
public class GrantsUnavailableException : ArmoniKException
{
  /// <summary>
  ///   Build the exception
  /// </summary>
  public GrantsUnavailableException()
  {
  }

  /// <summary>
  ///   Build the exception
  /// </summary>
  /// <param name="message">Why the grants could not be read</param>
  public GrantsUnavailableException(string message)
    : base(message)
  {
  }

  /// <summary>
  ///   Build the exception
  /// </summary>
  /// <param name="message">Why the grants could not be read</param>
  /// <param name="innerException">Failure of the read</param>
  public GrantsUnavailableException(string    message,
                                    Exception innerException)
    : base(message,
           innerException)
  {
  }
}
