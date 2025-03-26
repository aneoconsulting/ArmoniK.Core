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
using System.Collections.Generic;

namespace ArmoniK.Core.Common.Storage;

/// <summary>
/// </summary>
/// <param name="SessionId">Id of the session that produces and consumes this data</param>
/// <param name="ResultId">Unique Id of the result</param>
/// <param name="Name">Name to reference and access this result</param>
/// <param name="CreatedBy">Id of the task that created this result.</param>
/// <param name="OwnerTaskId">Id of the task that is responsible of generating this result.</param>
/// <param name="Status">Status of the result (can be Created, Completed or Aborted)</param>
/// <param name="DependentTasks">List of tasks that depend on this result.</param>
/// <param name="CreationDate">Date of creation of the current object.</param>
/// <param name="Size">Size of the result.</param>
/// <param name="OpaqueId">Opaque Identifier used by the object storage to refer to this result's data</param>
/// <param name="ManualDeletion">If the user is responsible for the deletion of the data in the underlying object storage</param>
public record Result(string       SessionId,
                     string       ResultId,
                     string       Name,
                     string       CreatedBy,
                     string       OwnerTaskId,
                     ResultStatus Status,
                     List<string> DependentTasks,
                     DateTime     CreationDate,
                     long         Size,
                     byte[]       OpaqueId,
                     bool         ManualDeletion)
{
  /// <summary>
  ///   Creates a copy of a <see cref="Result" /> and modify it according to given updates
  /// </summary>
  /// <param name="original">The object that will be copied</param>
  /// <param name="updates">A collection of field selector and their new values</param>
  public Result(Result                   original,
                UpdateDefinition<Result> updates)
    : this(original)
    => updates.ApplyTo(this);
}
