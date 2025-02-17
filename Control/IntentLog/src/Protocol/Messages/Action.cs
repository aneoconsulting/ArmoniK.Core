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

namespace ArmoniK.Core.Control.IntentLog.Protocol.Messages;

public enum Action
{
  /// <summary>
  ///   Unused
  /// </summary>
  Invalid = 0,

  /// <summary>
  ///   Open a new intent
  /// </summary>
  Open = 1,

  /// <summary>
  ///   Amend a previously opened intent
  /// </summary>
  Amend = 2,

  /// <summary>
  ///   Close with success a previously opened intent
  /// </summary>
  Close = 3,

  /// <summary>
  ///   Close with error a previously opened intent
  /// </summary>
  Abort = 4,

  Timeout = 5,

  Reset = 6,
}

public static class ActionExtensions
{
  public static bool IsFinal(this Action action)
    => action is Action.Close or Action.Abort or Action.Timeout or Action.Reset;
}
