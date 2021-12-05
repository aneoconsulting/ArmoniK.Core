// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System.Threading;

using ArmoniK.Core.gRPC.V1;

using JetBrains.Annotations;

namespace ArmoniK.Core.Storage
{
  [PublicAPI]
  public record QueueMessage(
    string MessageId,
    TaskId TaskId,
    CancellationToken CancellationToken);
}