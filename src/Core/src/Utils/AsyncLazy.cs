// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ArmoniK.Core.Utils
{
  public class AsyncLazy<T> : Lazy<Task<T>>
  {
    public AsyncLazy(Func<T> valueFactory) :
      base(() => Task.FromResult(valueFactory()))
    {
    }

    public AsyncLazy(Func<Task<T>> taskFactory) :
      base(taskFactory)
    {
    }

    public TaskAwaiter<T> GetAwaiter() => Value.GetAwaiter();
  }
}