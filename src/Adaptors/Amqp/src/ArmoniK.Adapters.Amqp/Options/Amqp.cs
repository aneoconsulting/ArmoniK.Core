// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

namespace ArmoniK.Adapters.Amqp.Options
{
  public class Amqp
  {
    public const string SettingSection = nameof(Amqp);

    public       string Address     { get; set; }
    public       int    MaxPriority { get; set; }
  }
}
