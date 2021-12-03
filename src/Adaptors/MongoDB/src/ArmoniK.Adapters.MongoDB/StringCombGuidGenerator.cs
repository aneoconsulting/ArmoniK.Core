using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.IdGenerators;

namespace ArmoniK.Adapters.MongoDB
{
  public class StringCombGuidGenerator : IIdGenerator
  {
    private static CombGuidGenerator Generator => CombGuidGenerator.Instance;

    /// <inheritdoc />
    public object GenerateId(object container, object document)
      => $"{Generator.GenerateId(container, document)}";

    /// <inheritdoc />
    public bool IsEmpty(object id) => id == null || ((string) id).Equals("00000000-0000-0000-0000-000000000000");
  }
}