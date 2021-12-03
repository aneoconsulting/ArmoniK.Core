// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Exceptions
{
  public class ArmoniKException : Exception
  {
    public ArmoniKException()
    {
    }

    public ArmoniKException(string message) : base(message)
    {
    }

    public ArmoniKException(string message, Exception innerException) : base(message, innerException)
    {
    }

    protected ArmoniKException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
  }
}