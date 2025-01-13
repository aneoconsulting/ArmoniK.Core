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

namespace ArmoniK.Core.Common.Storage;

public enum ResultStatus
{
  /// <summary>Result is in an unspecified state.</summary>
  Unspecified,

  /// <summary>Result is created and task is created, submitted or dispatched.</summary>
  Created,

  /// <summary>Result is completed with a completed task.</summary>
  Completed,

  /// <summary>Result is aborted.</summary>
  Aborted,

  /// <summary>Result data were deleted.</summary>
  DeletedData,
}
