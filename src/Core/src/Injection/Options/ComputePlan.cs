// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using JetBrains.Annotations;

namespace ArmoniK.Core.Injection.Options
{
  [PublicAPI]
  public class ComputePlan
  {
    public const string SettingSection = nameof(ComputePlan);

    public GrpcChannel GrpcChannel { get; set; }

    public int MessageBatchSize { get; set; }
  }
}