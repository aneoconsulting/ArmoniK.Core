// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Core
{
  [PublicAPI]
  public static class LogEvents
  {
    public static readonly EventId StartMessageProcessing = new(800, "Start message processing");
  }
}