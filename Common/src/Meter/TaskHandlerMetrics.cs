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

using ArmoniK.Core.Common.Pollster;
using ArmoniK.Utils;

namespace ArmoniK.Core.Common.Meter;

public class TaskHandlerMetrics
{
  public const string Name = "ArmoniK.Core.TaskHandlerMetrics";

  private readonly ConcurrentDictionary<string, (Counter<int> counter, Histogram<long> histogram)> instruments_ = new();
  private readonly System.Diagnostics.Metrics.Meter                                                meter_;
  private readonly KeyValuePair<string, object?>[]                                                 tags_;

  public TaskHandlerMetrics(IMeterFactory meterFactory)
  {
    tags_ = new KeyValuePair<string, object?>[]
            {
              new($"{Name}.{nameof(AgentIdentifier.OwnerPodId)}".ToLower(),
                  AgentIdentifier.OwnerPodId),
              new($"{Name}.{nameof(AgentIdentifier.OwnerPodName)}".ToLower(),
                  AgentIdentifier.OwnerPodName),
            };

    var options = new MeterOptions(Name)
                  {
                    Tags    = tags_,
                    Version = "1.0.0",
                  };

    meter_ = meterFactory.Create(options);
  }


  public IDisposable CountAndTime([CallerMemberName] string name = "")
  {
    var ins = instruments_.GetOrAdd(name,
                                    s =>
                                    {
                                      var counter = meter_.CreateCounter<int>($"task-handler-{s}",
                                                                              "Count",
                                                                              $"Number of {nameof(TaskHandler)}.{s} execution",
                                                                              tags_);
                                      var histogram = meter_.CreateHistogram<long>($"task-handler-{s}",
                                                                                   "milliseconds",
                                                                                   $"Duration needed to execute {nameof(TaskHandler)}.{s}",
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
