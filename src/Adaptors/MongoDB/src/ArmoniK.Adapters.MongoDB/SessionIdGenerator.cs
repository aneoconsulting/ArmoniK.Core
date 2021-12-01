// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;

namespace ArmoniK.Adapters.MongoDB
{
  public class SessionIdGenerator : IIdGenerator
  {
    private static CombGuidGenerator Generator => CombGuidGenerator.Instance;

    /// <inheritdoc />
    public object GenerateId(object container, object document)
      => $"{(document as SessionDataModel)?.IdTag}{Generator.GenerateId(container, document)}";

        /// <inheritdoc />
        public bool IsEmpty(object id) => id == null || ((string)id).EndsWith("00000000-0000-0000-0000-000000000000");
    }
}
