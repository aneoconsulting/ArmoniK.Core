// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System.Diagnostics;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;

using OpenTelemetry;

namespace ArmoniK.Core.ProfilingTools.OpenTelemetryExporter;

internal class MongoExporter : BaseExporter<Activity>
{
  private readonly IMongoClient                        client_;
  private readonly IMongoCollection<OpenTelemetryData> collection_;
  private readonly IMongoDatabase                      db_;
  private readonly ILogger                             logger_;
  private readonly IClientSessionHandle                session_;

  public MongoExporter(MongoExporterOptions options)
  {
    var template = "mongodb://{0}:{1}/{2}";
    var connectionString = string.Format(template,
                                         options.AgentHost,
                                         options.AgentPort,
                                         options.DatabaseName);
    var settings = MongoClientSettings.FromUrl(new MongoUrl(connectionString));
    settings.Scheme = ConnectionStringScheme.MongoDB;
    client_         = new MongoClient(settings);
    session_        = client_.StartSession();
    db_             = client_.GetDatabase(options.DatabaseName);
    db_.CreateCollection(session_,
                         "traces");
    collection_ = db_.GetCollection<OpenTelemetryData>("traces");
    logger_     = options.Logger;
    logger_.LogDebug("Mongo exporter created with connection : {connectionString}",
                     connectionString);
  }

  public override ExportResult Export(in Batch<Activity> batch)
  {
    try
    {
      foreach (var activity in batch)
      {
        collection_.InsertOne(session_,
                              activity.ToOpenTelemetryData());
      }

      return ExportResult.Success;
    }
    catch
    {
      return ExportResult.Failure;
    }
  }

  /// <inheritdoc />
  protected override void Dispose(bool disposing)
    => session_.Dispose();
}
