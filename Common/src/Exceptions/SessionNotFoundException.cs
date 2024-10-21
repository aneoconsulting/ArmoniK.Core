// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

[Serializable]
public class SessionNotFoundException : ArmoniKException
{
  public SessionNotFoundException()
  {
  }

  public SessionNotFoundException(bool deleted)
    => Deleted = deleted;

  public SessionNotFoundException(string message,
                                  bool   deleted = false)
    : base(message)
    => Deleted = deleted;

  public SessionNotFoundException(string    message,
                                  Exception innerException,
                                  bool      deleted = false)
    : base(message,
           innerException)
    => Deleted = deleted;

  public bool Deleted { get; }
}
