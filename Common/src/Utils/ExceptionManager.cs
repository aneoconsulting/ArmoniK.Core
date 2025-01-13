// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Diagnostics.CodeAnalysis;
using System.Threading;

using JetBrains.Annotations;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Common.Utils;

/// <summary>
///   Manage exceptions at the whole application level.
///   If too many exceptions are thrown, the application is stopped.
///   When the application is stopped, a grace delay is in place to let
///   all the running services to stop properly.
/// </summary>
public class ExceptionManager : IDisposable
{
  private readonly IHostApplicationLifetime applicationLifetime_;
  private readonly IList<IDisposable>       disposables_;

  [SuppressMessage("Usage",
                   "CA2213: Disposable fields must be disposed")]
  private readonly CancellationTokenSource earlyCts_;

  [SuppressMessage("Usage",
                   "CA2213: Disposable fields must be disposed")]
  private readonly CancellationTokenSource lateCts_;

  private readonly ILogger? logger_;
  private readonly int      maxError_;
  private          int      nbError_;

  /// <summary>
  ///   Build an ExceptionManager
  /// </summary>
  /// <param name="applicationLifetime">The lifetime of the application</param>
  /// <param name="logger">The logger for the ExceptionManager</param>
  /// <param name="options">Options for the ExceptionManager</param>
  public ExceptionManager(IHostApplicationLifetime   applicationLifetime,
                          ILogger<ExceptionManager>? logger  = null,
                          Options?                   options = null)
  {
    options              ??= new Options();
    applicationLifetime_ =   applicationLifetime;
    disposables_         =   new List<IDisposable>();
    earlyCts_            =   new CancellationTokenSource();
    lateCts_             =   earlyCts_;

    disposables_.Add(applicationLifetime.ApplicationStopping.Register(() =>
                                                                      {
                                                                        if (!earlyCts_.IsCancellationRequested)
                                                                        {
                                                                          logger_?.LogInformation("Application shut down has been triggered externally");
                                                                          earlyCts_.Cancel();
                                                                        }
                                                                      }));

    if (options.GraceDelay.Ticks > 0)
    {
      lateCts_ = CancellationTokenSource.CreateLinkedTokenSource(applicationLifetime.ApplicationStopped);
      disposables_.Add(earlyCts_.Token.Register(() => lateCts_.CancelAfter(options.GraceDelay)));
    }

    disposables_.Add(lateCts_.Token.Register(() =>
                                             {
                                               if (!applicationLifetime_.ApplicationStopped.IsCancellationRequested)
                                               {
                                                 logger_?.LogInformation("Grace delay has expired");
                                                 applicationLifetime_.StopApplication();
                                               }
                                             }));


    maxError_ = options.MaxError ?? int.MaxValue / 2;
    logger_   = logger;
  }

  /// <summary>
  ///   CancellationToken that is triggered as soon as the application is stopped,
  ///   or too many errors were thrown
  /// </summary>
  public CancellationToken EarlyCancellationToken
    => earlyCts_.Token;

  /// <summary>
  ///   CancellationToken that is triggered after a grace delay
  /// </summary>
  public CancellationToken LateCancellationToken
    => lateCts_.Token;

  /// <summary>
  ///   Whether there were too many errors
  /// </summary>
  public bool Failed { get; private set; }

  /// <inheritdoc />
  public void Dispose()
  {
    foreach (var disposable in disposables_)
    {
      disposable.Dispose();
    }

    // Cts are just cancelled instead of disposed in case a background task is waiting on them.
    // As the lifetime of this class is linked to the lifetime of the whole application,
    // It is benign to leak those two Cts.
    earlyCts_.Cancel();
    lateCts_.Cancel();
  }

  /// <summary>
  ///   Record an error at the application level
  /// </summary>
  /// <param name="logger">Logger used to log the error</param>
  /// <param name="e">Exception to log, if any</param>
  /// <param name="message">Message to log</param>
  /// <param name="args">Arguments for the log message</param>
  /// <remarks>
  ///   <para>
  ///     If <paramref name="logger" /> is null, the logger of <code>this</code> is used.
  ///   </para>
  ///   <para>
  ///     If the maximum number of errors is reached, it will trigger the <see cref="EarlyCancellationToken" />,
  ///     and make the following errors logged as Warnings instead of Errors.
  ///   </para>
  /// </remarks>
  public void RecordError(ILogger?                              logger,
                          Exception?                            e,
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
      Action<ILogger, Exception?, string, object?[]> log = nbError <= maxError_ + 1
                                                             ? LoggerExtensions.LogError
                                                             : LoggerExtensions.LogWarning;

      log.Invoke(logger,
                 e,
                 message,
                 args);
    }

    if (nbError == maxError_ + 1)
    {
      logger_?.LogCritical("Stop Application after too many errors");
      Failed = true;
      earlyCts_.Cancel();
    }
  }

  /// <summary>
  ///   Record a fatal error at the application level and trigger <see cref="EarlyCancellationToken" />
  /// </summary>
  /// <param name="logger">Logger used to log the error</param>
  /// <param name="e">Exception to log, if any</param>
  /// <param name="message">Message to log</param>
  /// <param name="args">Arguments for the log message</param>
  /// <remarks>
  ///   <para>
  ///     If <paramref name="logger" /> is null, the logger of <code>this</code> is used.
  ///   </para>
  ///   <para>
  ///     Following errors are logged as Warnings instead of Errors
  ///   </para>
  /// </remarks>
  public void FatalError(ILogger?                              logger,
                         Exception?                            e,
                         [StructuredMessageTemplate] string    message,
                         params                      object?[] args)
  {
    logger ??= logger_;

    var nbError = Interlocked.Exchange(ref nbError_,
                                       maxError_ + 1);

    if (logger is not null)
    {
      using var scope = logger.BeginScope("Fatal Exception");
      Action<ILogger, Exception?, string, object?[]> log = nbError <= maxError_
                                                             ? LoggerExtensions.LogCritical
                                                             : LoggerExtensions.LogWarning;

      log.Invoke(logger,
                 e,
                 message,
                 args);
    }

    Failed = true;
    earlyCts_.Cancel();
  }

  /// <summary>
  ///   Decrease the number of recorded errors to indicate that the application is behaving correctly.
  /// </summary>
  /// <param name="logger">Logger used to log the success</param>
  /// <remarks>
  ///   <para>
  ///     If <paramref name="logger" /> is null, the logger of <code>this</code> is used.
  ///   </para>
  ///   <para>
  ///     If the ExceptionManager is already in Failed state, this call does nothing.
  ///   </para>
  /// </remarks>
  public void RecordSuccess(ILogger? logger = null)
  {
    logger ??= logger_;

    var nbError = nbError_;
    int previousNbError;
    do
    {
      if (nbError == 0 || nbError > maxError_)
      {
        return;
      }

      previousNbError = nbError;
      nbError = Interlocked.CompareExchange(ref nbError_,
                                            nbError - 1,
                                            previousNbError);
    } while (nbError != previousNbError);


    logger?.LogTrace("Success has been recorded, decrementing number of errors: {NbError}/{MaxError}",
                     nbError - 1,
                     maxError_);
  }

  /// <summary>
  ///   Stop the Application without any error
  /// </summary>
  /// <param name="logger">Logger used to log the success</param>
  /// <param name="message">Message to be logged</param>
  /// <param name="args">Arguments for the log message</param>
  /// <remarks>
  ///   <para>
  ///     If <paramref name="logger" /> is null, the logger of <code>this</code> is used.
  ///   </para>
  ///   <para>
  ///     The only difference with stopping the application directly is the log message.
  ///     Using this function indicates the reason of the shutdown, instead of indicating an external shutdown.
  ///   </para>
  /// </remarks>
  public void Stop(ILogger?                              logger,
                   [StructuredMessageTemplate] string    message,
                   params                      object?[] args)
  {
    if (applicationLifetime_.ApplicationStopping.IsCancellationRequested)
    {
      return;
    }

    logger ??= logger_;

    earlyCts_.Cancel();
    applicationLifetime_.StopApplication();

    if (string.IsNullOrWhiteSpace(message))
    {
      message = "Application shutdown has been triggered internally";
    }

    // ReSharper disable once TemplateIsNotCompileTimeConstantProblem
    logger?.LogInformation(message,
                           args);
  }

  /// <summary>
  ///   Options for the ExceptionManger
  /// </summary>
  /// <param name="GraceDelay">
  ///   Delay between the <see cref="ExceptionManager.EarlyCancellationToken" /> and the
  ///   <see cref="ExceptionManager.LateCancellationToken" />
  /// </param>
  /// <param name="MaxError">Maximum number of allowed errors</param>
  public record Options(TimeSpan GraceDelay = default,
                        int?     MaxError   = null);
}
