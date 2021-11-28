// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using JetBrains.Annotations;

namespace ArmoniK.Adapters.MongoDB.Options
{
  [PublicAPI]
  public class LeaseProvider
  {
    public const string SettingSection = nameof(MongoDB)+":"+nameof(LeaseProvider);

    public TimeSpan AcquisitionPeriod   { get; set; } = TimeSpan.FromMinutes(2);
    public TimeSpan AcquisitionDuration { get; set; } = TimeSpan.FromMinutes(5);
  }
}
