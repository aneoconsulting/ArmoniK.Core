// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using JetBrains.Annotations;

namespace ArmoniK.Adapters.MongoDB.Options
{
  [PublicAPI]
  public class MongoDB
  {
    public const string SettingSection = nameof(MongoDB);

    public string ConnectionString { get; set; }
    
    public string DatabaseName { get; set; } = "ArmoniK";

    public TimeSpan DataRetention { get; set; } = TimeSpan.FromDays(15);

    public TableStorage TableStorage { get; set; }

    public LeaseProvider LeaseProvider { get; set; }

    public ObjectStorage ObjectStorage { get; set; }

    public QueueStorage QueueStorage { get; set; }
  }
}
