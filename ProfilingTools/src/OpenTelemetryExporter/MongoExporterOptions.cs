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
using System.Diagnostics;
using System.Net.Http;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using OpenTelemetry;
using OpenTelemetry.Trace;

namespace ArmoniK.Core.ProfilingTools.OpenTelemetryExporter;

public class MongoExporterOptions
{
  internal static readonly Func<HttpClient> DefaultHttpClientFactory = () => new HttpClient();

  /// <summary>
  ///   Gets or sets the MongoDB host. Default value: localhost.
  /// </summary>
  public string AgentHost { get; set; } = "localhost";

  /// <summary>
  ///   Gets or sets the MongoDB database name. Default value: traces.
  /// </summary>
  public string DatabaseName { get; set; } = "traces";

  /// <summary>
  ///   Gets or sets the MongoDB port. Default value: 27017.
  /// </summary>
  public int AgentPort { get; set; } = 27017;

  /// <summary>
  ///   Gets or sets the export processor type to be used with MongoDB Exporter. The default value is <see cref="Batch{T}" />
  ///   .
  /// </summary>
  public ExportProcessorType ExportProcessorType { get; set; } = ExportProcessorType.Batch;

  /// <summary>
  ///   Gets or sets the BatchExportProcessor options. Ignored unless ExportProcessorType is BatchExporter.
  /// </summary>
  public BatchExportProcessorOptions<Activity> BatchExportProcessorOptions { get; set; } = new BatchExportActivityProcessorOptions();

  /// <summary>
  ///   Gets or sets the Logger. Can be null if not used.
  /// </summary>
  public ILogger Logger { get; set; } = NullLogger.Instance;
}
