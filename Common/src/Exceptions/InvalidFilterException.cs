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
///   The filters or the sort of a request cannot be converted into a query
/// </summary>
/// <remarks>
///   An error of the request, not of the service: it is answered as an invalid argument and does not count toward the
///   errors that make the service unhealthy
/// </remarks>
[Serializable]
public class InvalidFilterException : ArmoniKException
{
  /// <summary>
  ///   Build the exception
  /// </summary>
  public InvalidFilterException()
  {
  }

  /// <summary>
  ///   Build the exception
  /// </summary>
  /// <param name="message">What is invalid in the request</param>
  public InvalidFilterException(string message)
    : base(message)
  {
  }

  /// <summary>
  ///   Build the exception
  /// </summary>
  /// <param name="message">What is invalid in the request</param>
  /// <param name="innerException">The failure of the conversion</param>
  public InvalidFilterException(string    message,
                                Exception innerException)
    : base(message,
           innerException)
  {
  }
}
