using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Exceptions
{
  [Serializable]
  public class ComputeLibInitException : ArmoniKException
  {
    public ComputeLibInitException()
    {
    }

    public ComputeLibInitException(string message) : base(message)
    {
    }

    public ComputeLibInitException(string message, Exception innerException) : base(message, innerException)
    {
    }

    protected ComputeLibInitException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
  }
}