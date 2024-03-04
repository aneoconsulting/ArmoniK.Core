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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;

using ArmoniK.Utils;

namespace ArmoniK.Core.Common.Meter;

public class FunctionExecutionMetrics<TIns>
{
  private readonly string                                                                          className_;
  private readonly ConcurrentDictionary<string, (Counter<int> counter, Histogram<long> histogram)> instruments_ = new();
  private readonly System.Diagnostics.Metrics.Meter                                                meter_;
  private readonly IReadOnlyDictionary<string, object?>                                            tags_;

  public FunctionExecutionMetrics(MeterHolder holder)
  {
    className_ = typeof(TIns).Name;
    meter_     = holder.Meter;
    tags_      = holder.Tags;
  }


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
