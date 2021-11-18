// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using ArmoniK.Core.gRPC.V1;

namespace ArmoniK.Core.Storage
{
  public record QueueMessage(
    string      MessageId,
    TaskId      TaskId,
    bool        HasTaskOptions,
    TaskOptions TaskOptions,
    bool        HasTaskPayload,
    Payload     Payload);
}
