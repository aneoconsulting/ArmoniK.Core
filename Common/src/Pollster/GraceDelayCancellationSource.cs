// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading;

using JetBrains.Annotations;

using Microsoft.Extensions.Hosting;

namespace ArmoniK.Core.Common.Pollster;

[UsedImplicitly]
public class GraceDelayCancellationSource : IDisposable
{
  public readonly  CancellationTokenSource       DelayedCancellationTokenSource;
  public readonly  CancellationTokenSource       LifetimeCancellationTokenSource;
  private readonly CancellationTokenRegistration registration_;

  public GraceDelayCancellationSource(IHostApplicationLifetime   lifetime,
                                      Injection.Options.Pollster pollsterOptions)
  {
    LifetimeCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(lifetime.ApplicationStopping);
    DelayedCancellationTokenSource  = new CancellationTokenSource();
    registration_                   = LifetimeCancellationTokenSource.Token.Register(() => DelayedCancellationTokenSource.CancelAfter(pollsterOptions.GraceDelay));
  }

  public void Dispose()
  {
    registration_.Dispose();
    LifetimeCancellationTokenSource.Dispose();
    DelayedCancellationTokenSource.Dispose();
  }
}
