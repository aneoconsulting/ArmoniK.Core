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
using System.Collections.Generic;
using System.Threading;

using JetBrains.Annotations;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Utils;

public class ExceptionManager : IDisposable
{
  private readonly IList<IDisposable>      disposables_;
  private readonly CancellationTokenSource graceCts_;
  private readonly ILogger?                logger_;
  private readonly int                     maxError_;
  private          int                     nbError_;

  public ExceptionManager(IHostApplicationLifetime   applicationLifetime,
                          TimeSpan                   graceDelay,
                          ILogger<ExceptionManager>? logger   = null,
                          int                        maxError = int.MaxValue / 2)
  {
    disposables_ = new List<IDisposable>();
    graceCts_    = CancellationTokenSource.CreateLinkedTokenSource(applicationLifetime.ApplicationStopping);
    var cts = graceCts_;

    disposables_.Add(applicationLifetime.ApplicationStopping.Register(() =>
                                                                      {
                                                                        if (!graceCts_.IsCancellationRequested)
                                                                        {
                                                                          logger_?.LogInformation("Application shut down has been triggered externally");
                                                                        }
                                                                      }));

    if (graceDelay.Ticks > 0)
    {
      cts = CancellationTokenSource.CreateLinkedTokenSource(applicationLifetime.ApplicationStopped);
      disposables_.Add(cts);
      disposables_.Add(graceCts_.Token.Register(() => cts.CancelAfter(graceDelay)));
    }

    CancellationToken = cts.Token;

    disposables_.Add(cts.Token.Register(() =>
                                        {
                                          if (!applicationLifetime.ApplicationStopped.IsCancellationRequested)
                                          {
                                            logger_?.LogInformation("Grace delay has expired");
                                            applicationLifetime.StopApplication();
                                          }
                                        }));


    maxError_ = maxError;
    logger_   = logger;
  }

  public CancellationToken GraceCancellationToken
    => graceCts_.Token;

  public CancellationToken CancellationToken { get; }

  public void Dispose()
  {
    foreach (var disposable in disposables_)
    {
      disposable.Dispose();
    }

    graceCts_.Dispose();
  }

  public void RecordError(ILogger?                              logger,
                          Exception                             e,
                          [StructuredMessageTemplate] string    message,
                          params                      object?[] args)
  {
    logger ??= logger_;

    var nbError = Interlocked.Increment(ref nbError_);

    if (logger is not null)
    {
      using var scope = logger.BeginScope("Exception #{NbError}/{MaxError}",
                                          nbError,
                                          maxError_);
      Action<ILogger, Exception, string, object?[]> log = nbError <= maxError_
                                                            ? LoggerExtensions.LogError
                                                            : LoggerExtensions.LogDebug;

      log.Invoke(logger,
                 e,
                 message,
                 args);
    }

    if (nbError == maxError_)
    {
      logger_?.LogCritical("Stop Application after too many errors");
      graceCts_.Cancel();
    }
  }

  public void FatalError(ILogger?                              logger,
                         Exception                             e,
                         [StructuredMessageTemplate] string    message,
                         params                      object?[] args)
  {
    logger ??= logger_;

    var nbError = Interlocked.Exchange(ref nbError_,
                                       maxError_ + 1);

    if (logger is not null)
    {
      using var scope = logger.BeginScope("Fatal Exception");
      Action<ILogger, Exception, string, object?[]> log = nbError <= maxError_
                                                            ? LoggerExtensions.LogCritical
                                                            : LoggerExtensions.LogDebug;

      log.Invoke(logger,
                 e,
                 message,
                 args);
    }

    graceCts_.Cancel();
  }

  public void RecordSuccess()
  {
    var nbError = nbError_;
    int previousNbError;
    do
    {
      previousNbError = nbError;
      nbError = Interlocked.CompareExchange(ref nbError_,
                                            nbError == 0
                                              ? 0
                                              : nbError - 1,
                                            previousNbError);
    } while (nbError != previousNbError);

    logger_?.LogTrace("Success has been recorded, decrementing number of errors");
  }
}
