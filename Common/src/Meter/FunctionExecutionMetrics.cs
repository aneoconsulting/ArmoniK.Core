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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

using ArmoniK.Utils;

namespace ArmoniK.Core.Common.Meter;

/// <summary>
///   Provides metrics collection for function execution, tracking both count and duration.
/// </summary>
/// <typeparam name="TIns">The type whose methods will be measured.</typeparam>
/// <remarks>
///   This class creates and manages counters and histograms for method calls,
///   allowing automatic instrumentation of methods with minimal code changes.
///   The metrics include both invocation counts and execution durations.
/// </remarks>
public class FunctionExecutionMetrics<TIns>
{
  private readonly string                                                                          className_;
  private readonly ConcurrentDictionary<string, (Counter<int> counter, Histogram<long> histogram)> instruments_ = new();
  private readonly System.Diagnostics.Metrics.Meter                                                meter_;
  private readonly IReadOnlyDictionary<string, object?>                                            tags_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="FunctionExecutionMetrics{TIns}" /> class.
  /// </summary>
  /// <param name="holder">The meter holder that provides the meter and common tags.</param>
  public FunctionExecutionMetrics(MeterHolder holder)
  {
    className_ = typeof(TIns).Name;
    meter_     = holder.Meter;
    tags_      = holder.Tags;
  }

  /// <summary>
  ///   Creates metrics for a method call and starts timing its execution.
  /// </summary>
  /// <param name="callerName">
  ///   The name of the calling method, automatically populated by the compiler
  ///   when using the <see cref="CallerMemberNameAttribute" />.
  /// </param>
  /// <returns>
  ///   An <see cref="IDisposable" /> that, when disposed, will record the method's completion
  ///   and update the relevant metrics.
  /// </returns>
  /// <remarks>
  ///   This method is designed to be used with a using statement at the start of a method:
  ///   <code>
  /// using var _ = metrics.CountAndTime();
  /// </code>
  /// </remarks>
  public IDisposable CountAndTime([CallerMemberName] string callerName = "")
  {
    var ins = instruments_.GetOrAdd(callerName,
                                    s =>
                                    {
                                      var counter = meter_.CreateCounter<int>($"{className_}-{s}",
                                                                              "Count",
                                                                              $"Number of {className_}.{s} execution",
                                                                              tags_);
                                      var histogram = meter_.CreateHistogram<long>($"{className_}-{s}",
                                                                                   "milliseconds",
                                                                                   $"Duration needed to execute {className_}.{s}",
                                                                                   tags_);

                                      return (counter, histogram);
                                    });
    var watch = Stopwatch.StartNew();
    return new Deferrer(() =>
                        {
                          watch.Stop();
                          ins.counter.Add(1);
                          ins.histogram.Record(watch.ElapsedMilliseconds);
                        });
  }
}
