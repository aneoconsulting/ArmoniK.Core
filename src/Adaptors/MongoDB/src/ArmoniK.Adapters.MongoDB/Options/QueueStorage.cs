// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using JetBrains.Annotations;

namespace ArmoniK.Adapters.MongoDB.Options
{
  [PublicAPI]
  public class QueueStorage
  {
    public const string SettingSection = nameof(MongoDB) + ":" + nameof(QueueStorage);

    public TimeSpan LockRefreshPeriodicity { get; set; } = TimeSpan.FromMinutes(2);

    public TimeSpan PollPeriodicity { get; set; } = TimeSpan.FromSeconds(5);

    public TimeSpan LockRefreshExtension { get; set; } = TimeSpan.FromMinutes(5);
  }
}