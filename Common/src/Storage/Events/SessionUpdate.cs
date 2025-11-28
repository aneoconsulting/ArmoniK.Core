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

namespace ArmoniK.Core.Common.Storage.Events;

/// Defines what triggered the SessionUpdate
public enum SessionUpdateType
{
  /// Session was newly created
  Create,
  /// Session status was updated
  Update,
  /// Session was deleted
  Delete,
}

/// Holds the basic information about a session that has changed
public record SessionUpdate(
    string SessionId,
    SessionStatus Status,
    SessionUpdateType Type)
{ }
