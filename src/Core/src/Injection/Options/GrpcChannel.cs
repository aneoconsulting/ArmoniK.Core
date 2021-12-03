// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using JetBrains.Annotations;

namespace ArmoniK.Core.Injection.Options
{
  [PublicAPI]
  public class GrpcChannel
  {
    public const string SettingSection = ComputePlan.SettingSection + ":" + nameof(GrpcChannel);

    public string Address { get; set; }

    public GrpcSocketType SocketType { get; set; } = GrpcSocketType.Web;
  }
}