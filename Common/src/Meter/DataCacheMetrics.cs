// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-2026. All rights reserved.
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

using System.Collections.Generic;
using System.Diagnostics.Metrics;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.Meter;

/// <summary>
///   Metrics of the data the tasks of an agent need: served by its cache, or fetched from the object storage.
///   They measure the benefit of the cache, and of the queues placing tasks next to their data, whatever the
///   object storage.
/// </summary>
/// <remarks>
///   Measurements about input data are tagged with <c>kind</c>: <c>payload</c> or <c>dependency</c>.
/// </remarks>
[UsedImplicitly]
public sealed class DataCacheMetrics
{
  private static readonly KeyValuePair<string, object?> Payload    = new("kind", "payload");
  private static readonly KeyValuePair<string, object?> Dependency = new("kind", "dependency");

  private readonly Counter<long>   evictedBytes_;
  private readonly Histogram<long> fetchDuration_;
  private readonly Counter<long>   fetchedBytes_;
  private readonly Counter<long>   hitBytes_;
  private readonly Counter<long>   hits_;
  private readonly Counter<long>   misses_;
  private readonly Counter<long>   storedBytes_;

  /// <summary>
  ///   Creates the instruments on the meter of the agent.
  /// </summary>
  /// <param name="holder">The meter holder that provides the meter and common tags.</param>
  public DataCacheMetrics(MeterHolder holder)
  {
    var meter = holder.Meter;
    var tags  = holder.Tags;
    hits_ = meter.CreateCounter<long>("agent_cache_hits",
                                      "{data}",
                                      "Input data of tasks found in the agent cache",
                                      tags);
    misses_ = meter.CreateCounter<long>("agent_cache_misses",
                                        "{data}",
                                        "Input data of tasks fetched from the object storage",
                                        tags);
    hitBytes_ = meter.CreateCounter<long>("agent_cache_hit_bytes",
                                          "By",
                                          "Bytes of input data served by the agent cache",
                                          tags);
    fetchedBytes_ = meter.CreateCounter<long>("agent_data_fetched_bytes",
                                              "By",
                                              "Bytes of input data fetched from the object storage",
                                              tags);
    fetchDuration_ = meter.CreateHistogram<long>("agent_data_fetch_duration",
                                                 "ms",
                                                 "Duration of the fetch of the input data of a task missing from the cache",
                                                 tags);
    storedBytes_ = meter.CreateCounter<long>("agent_cache_stored_bytes",
                                             "By",
                                             "Bytes of data put in the agent cache: fetched input data and task outputs",
                                             tags);
    evictedBytes_ = meter.CreateCounter<long>("agent_cache_evicted_bytes",
                                              "By",
                                              "Bytes of data evicted from the agent cache",
                                              tags);
  }

  private static KeyValuePair<string, object?> Kind(bool payload)
    => payload
         ? Payload
         : Dependency;

  /// <summary>
  ///   An input data was served by the cache.
  /// </summary>
  /// <param name="payload">Whether the data is the payload of the task</param>
  /// <param name="bytes">Size of the data</param>
  public void Hit(bool payload,
                  long bytes)
  {
    hits_.Add(1,
              Kind(payload));
    hitBytes_.Add(bytes,
                  Kind(payload));
  }

  /// <summary>
  ///   An input data was fetched from the object storage.
  /// </summary>
  /// <param name="payload">Whether the data is the payload of the task</param>
  /// <param name="bytes">Size of the data</param>
  public void Miss(bool payload,
                   long bytes)
  {
    misses_.Add(1,
                Kind(payload));
    fetchedBytes_.Add(bytes,
                      Kind(payload));
  }

  /// <summary>
  ///   The input data missing from the cache were fetched.
  /// </summary>
  /// <param name="milliseconds">Duration of the fetch</param>
  public void Fetched(long milliseconds)
    => fetchDuration_.Record(milliseconds);

  /// <summary>
  ///   Data were put in the cache.
  /// </summary>
  /// <param name="bytes">Size of the data</param>
  public void Stored(long bytes)
    => storedBytes_.Add(bytes);

  /// <summary>
  ///   Data were evicted from the cache.
  /// </summary>
  /// <param name="bytes">Size of the data</param>
  public void Evicted(long bytes)
    => evictedBytes_.Add(bytes);
}
