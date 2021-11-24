// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

using JetBrains.Annotations;

namespace ArmoniK.Compute.PollingAgent
{
  [PublicAPI]
  public record Configuration(string ComputeServiceAddress)
  {

    // to load from a terraform generated configuration file
    public static Configuration LoadFromFile(string filename) => throw new NotImplementedException($"{filename}");
  }
}
