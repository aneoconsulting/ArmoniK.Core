#nullable enable

using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Exceptions
{
  [Serializable]
  public class AppSettingsException : ArmoniKException
  {
    public AppSettingsException()
    {
    }

    public AppSettingsException(string? message) : base(message)
    {
    }

    public AppSettingsException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    protected AppSettingsException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
  }
}