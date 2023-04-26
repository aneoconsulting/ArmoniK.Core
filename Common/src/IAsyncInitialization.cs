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

using System.Threading.Tasks;

namespace ArmoniK.Core.Common;

/// <summary>
///   Marks a type as requiring asynchronous initialization
///   and provides the result of that initialization.
/// </summary>
public interface IAsyncInitialization<T>
{
  /// <summary>
  ///   The result of the asynchronous initialization of this instance.
  /// </summary>
  Task<T> Initialization { get; }
}
