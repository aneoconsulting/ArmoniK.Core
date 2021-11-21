// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public record Optional<T>(bool HasValue = false, T Value = null) where T : class
  {
    public Optional(T value):this(true, value){}
  }
}
