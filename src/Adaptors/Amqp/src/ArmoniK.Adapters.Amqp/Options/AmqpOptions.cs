// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

namespace ArmoniK.Adapters.Amqp.Options
{
  public class AmqpOptions
  {
    public const string SettingSection = nameof(AmqpOptions);

    public       string Address     { get; set; }
    public       int    MaxPriority { get; set; }
  }
}
