using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Exceptions
{
  public class TaskCanceledException : ArmoniKException
  {
    public TaskCanceledException()
    {
    }

    public TaskCanceledException(string message) : base(message)
    {
    }

    public TaskCanceledException(string message, Exception innerException) : base(message, innerException)
    {
    }

    protected TaskCanceledException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
  }
}