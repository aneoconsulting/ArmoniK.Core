// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Diagnostics;

namespace ArmoniK.Core.ProfilingTools.OpenTelemetryExporter;

public static class ActivityExt
{
  public static OpenTelemetryData ToOpenTelemetryData(this Activity activity)
    => new(activity.Id ?? "",
           activity.Baggage,
           activity.Duration,
           activity.Tags,
           activity.Context.SpanId.ToString(),
           activity.Context.TraceId.ToString(),
           activity.ParentId ?? "",
           activity.ParentSpanId.ToString(),
           activity.RootId ?? "",
           activity.DisplayName,
           activity.Source.Name,
           activity.StartTimeUtc);
}

public record OpenTelemetryData(string                                     ActivityId,
                                IEnumerable<KeyValuePair<string, string?>> Baggage,
                                TimeSpan                                   Duration,
                                IEnumerable<KeyValuePair<string, string?>> Tags,
                                string                                     SpanId,
                                string                                     TraceId,
                                string                                     ParentId,
                                string                                     ParentSpanId,
                                string                                     RootId,
                                string                                     DisplayName,
                                string                                     SourceName,
                                DateTime                                   StartTime);
