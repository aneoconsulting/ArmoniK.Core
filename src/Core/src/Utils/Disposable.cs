// This file is part of ArmoniK project.
// 
// Copyright (c) ANEO. All rights reserved.
//   W. Kirschenmann <wkirschenmann@aneo.fr>

using System;

namespace ArmoniK.Core.Utils
{
  internal static class Disposable
  {
    private class DisposableImpl : IDisposable
    {
      private readonly Action action_;

      public DisposableImpl(Action action) => action_ = action;

      /// <inheritdoc />
      public void Dispose()
      {
        action_();
      }
    }

    public static IDisposable Create(Action action) => new DisposableImpl(action);
  }
}
