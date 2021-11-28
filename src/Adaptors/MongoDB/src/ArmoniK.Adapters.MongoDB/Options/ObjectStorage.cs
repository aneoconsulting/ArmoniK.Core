// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using JetBrains.Annotations;

namespace ArmoniK.Adapters.MongoDB.Options
{
  [PublicAPI]
  public class ObjectStorage
  {
    public const string SettingSection = nameof(MongoDB)+":"+nameof(ObjectStorage);

    public int ChunkSize { get; set; } = 14500000;
  }
}
