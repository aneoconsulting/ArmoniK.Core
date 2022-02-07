﻿// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

using ArmoniK.Core.Common.Storage;

namespace ArmoniK.Core.Adapters.Memory;

public class Dispatch : IDispatch
{
  /// <inheritdoc />
  public string Id { get; init; }

  /// <inheritdoc />
  public string TaskId { get; init; }

  /// <inheritdoc />
  public int Attempt { get; set; }

  /// <inheritdoc />
  public DateTime TimeToLive { get; set; }

  public ConcurrentBag<StatusTime> Statuses { get; } = new();

  /// <inheritdoc />
  IEnumerable<StatusTime> IDispatch.Statuses => Statuses;

  /// <inheritdoc />
  public DateTime CreationDate { get; } = DateTime.UtcNow;

  /// <inheritdoc />
  public string SessionId { get; set; }
}
