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

using ArmoniK.Core.Common.Exceptions;

namespace ArmoniK.Core.Common.gRPC;

/// <summary>
///   Conversion of the filters and sorts of a request into a query
/// </summary>
internal static class FilterConversion
{
  /// <summary>
  ///   Run the conversion of the filters or the sort of a request
  /// </summary>
  /// <remarks>
  ///   The conversion reads nothing but the request: a field left unset, an unspecified field or a condition that does
  ///   not apply to the type of its field makes it fail, which is an error of the request
  /// </remarks>
  /// <typeparam name="T">Type of the query</typeparam>
  /// <param name="conversion">The conversion</param>
  /// <returns>
  ///   The query
  /// </returns>
  /// <exception cref="InvalidFilterException">The request cannot be converted</exception>
  internal static T Convert<T>(Func<T> conversion)
  {
    try
    {
      return conversion();
    }
    catch (Exception e) when (e is ArgumentException or InvalidOperationException or NullReferenceException or NotImplementedException
                                   or NotSupportedException)
    {
      throw new InvalidFilterException("The filters or the sort of the request are invalid",
                                       e);
    }
  }
}
