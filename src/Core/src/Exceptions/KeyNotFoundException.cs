// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Runtime.Serialization;

namespace ArmoniK.Core.Exceptions
{
  public class KeyNotFoundException : ArmoniKException
  {
    public KeyNotFoundException()
    {
    }

    public KeyNotFoundException(string message) : base(message)
    {
    }

    public KeyNotFoundException(string message, Exception innerException) : base(message, innerException)
    {
    }

    protected KeyNotFoundException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
  }
}