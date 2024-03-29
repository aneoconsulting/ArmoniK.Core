﻿// This file is part of the ArmoniK project
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

using JetBrains.Annotations;

namespace ArmoniK.Core.Adapters.MongoDB.Options;

[PublicAPI]
public class QueueStorage
{
  public const string SettingSection = nameof(MongoDB) + ":" + nameof(QueueStorage);

  public TimeSpan LockRefreshPeriodicity { get; set; } = TimeSpan.FromMinutes(2);

  public TimeSpan PollPeriodicity { get; set; } = TimeSpan.FromSeconds(5);

  public TimeSpan LockRefreshExtension { get; set; } = TimeSpan.FromMinutes(5);
}
