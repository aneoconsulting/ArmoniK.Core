using JetBrains.Annotations;

namespace ArmoniK.Core.Injection
{
  [PublicAPI]
  public class Components
  {
    public const string SettingSection = nameof(Components);

    public string TableStorage { get; set; }
    public string QueueStorage { get; set; }
    public string LeaseProvider { get; set; }
    public string ObjectStorage { get; set; }
  }
}
