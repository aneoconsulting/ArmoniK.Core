// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Exceptions
{
  public class TimeoutException : ArmoniKException
  {
    public TimeoutException()
    {
    }

    public TimeoutException(string message) : base(message)
    {
    }

    public TimeoutException(string message, Exception innerException) : base(message, innerException)
    {
    }

    protected TimeoutException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
  }
}