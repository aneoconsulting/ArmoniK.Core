using System.Text;
using System.Text.Json;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Common
{
  public class TestPayload
  {
    public enum TaskType
    {
      Result,
      Undefined,
      None,
      Compute,
      Error,
      Transfer,
      DatadepTransfer,
      DatadepCompute,
      ReturnFailed,
    }

    public byte [] DataBytes { get; set; }

    public TaskType Type { get; set; }

    public string ResultKey { get; set; }

    public byte[] Serialize()
    {
      var jsonString = JsonSerializer.Serialize(this);
      return Encoding.ASCII.GetBytes(StringToBase64(jsonString));
    }

    public static TestPayload? Deserialize(byte[] payload)
    {
      if (payload == null || payload.Length == 0)
        return new TestPayload
        {
          Type    = TaskType.Undefined,
        };

      var str = Encoding.ASCII.GetString(payload);
      return JsonSerializer.Deserialize<TestPayload>(Base64ToString(str));
    }

    private static string StringToBase64(string serializedJson)
    {
      var serializedJsonBytes       = Encoding.UTF8.GetBytes(serializedJson);
      var serializedJsonBytesBase64 = Convert.ToBase64String(serializedJsonBytes);
      return serializedJsonBytesBase64;
    }

    private static string Base64ToString(string base64)
    {
      var c = Convert.FromBase64String(base64);
      return Encoding.ASCII.GetString(c);
    }
  }
}