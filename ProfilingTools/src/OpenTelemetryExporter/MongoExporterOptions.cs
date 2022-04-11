using System;
using System.Diagnostics;
using System.Net.Http;

using Microsoft.Extensions.Logging;

using OpenTelemetry;
using OpenTelemetry.Trace;

namespace ArmoniK.Core.ProfilingTools.OpenTelemetryExporter;

public class MongoExporterOptions
{
  internal static readonly Func<HttpClient> DefaultHttpClientFactory = () => new HttpClient();

  public MongoExporterOptions()
  {

  }

  /// <summary>
  /// Gets or sets the MongoDB host. Default value: localhost.
  /// </summary>
  public string AgentHost { get; set; } = "localhost";

  /// <summary>
  /// Gets or sets the MongoDB database name. Default value: traces.
  /// </summary>
  public string DatabaseName { get; set; } = "traces";

  /// <summary>
  /// Gets or sets the MongoDB port. Default value: 27017.
  /// </summary>
  public int AgentPort { get; set; } = 27017;

  /// <summary>
  /// Gets or sets the export processor type to be used with MongoDB Exporter. The default value is <see cref="Batch{T}"/>.
  /// </summary>
  public ExportProcessorType ExportProcessorType { get; set; } = ExportProcessorType.Batch;

  /// <summary>
  /// Gets or sets the BatchExportProcessor options. Ignored unless ExportProcessorType is BatchExporter.
  /// </summary>
  public BatchExportProcessorOptions<Activity> BatchExportProcessorOptions { get; set; } = new BatchExportActivityProcessorOptions();

  /// <summary>
  /// Gets or sets the Logger. Can be null if not used.
  /// </summary>
  public ILogger Logger { get; set; } = null;

}