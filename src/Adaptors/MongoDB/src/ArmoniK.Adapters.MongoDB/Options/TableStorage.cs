// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using JetBrains.Annotations;

namespace ArmoniK.Adapters.MongoDB.Options
{
  [PublicAPI]
  public class TableStorage
  {
    public const string SettingSection = nameof(MongoDB)+":"+nameof(TableStorage);

    public TimeSpan PollingDelay { get; set; } = TimeSpan.FromSeconds(5);
  }
}
