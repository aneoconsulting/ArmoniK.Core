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

using ArmoniK.Utils.DocAttribute;

namespace ArmoniK.Core.Adapters.Gcs.Options;

/// <summary>
///   Represents the configuration settings for connecting to Google Cloud Storage.
/// </summary>
[ExtractDocumentation("Options for Gcs")]
public class Gcs
{
  /// <summary>
  ///   The name of the configuration section for Gcs Object Storage settings.
  /// </summary>
  public const string SettingSection = nameof(Gcs);

  /// <summary>
  ///   Google Cloud project identifier owning the bucket.
  /// </summary>
  public string ProjectId { get; set; } = "";

  /// <summary>
  ///   Name of the bucket that the application will read from and write to.
  /// </summary>
  public string BucketName { get; set; } = "";

  /// <summary>
  ///   Path to a service-account JSON credentials file.
  ///   When empty, Application Default Credentials are used.
  /// </summary>
  public string CredentialsFilePath { get; set; } = "";

  /// <summary>
  ///   Custom base URI used to target an emulator (e.g. fake-gcs-server).
  ///   When empty, the real Google Cloud Storage endpoint is used.
  /// </summary>
  public string EmulatorEndpoint { get; set; } = "";

  /// <summary>
  ///   Number of tasks to be used in parallel execution (deletes, metadata lookups).
  /// </summary>
  public int DegreeOfParallelism { get; set; }

  /// <summary>
  ///   Size of one chunk when downloading an object by chunks
  /// </summary>
  public int ChunkDownloadSize { get; set; } = 2 * 1024 * 1024;

  /// <summary>
  ///   Number of retry in case of a connection error
  /// </summary>
  public int MaxRetry { get; set; } = 5;

  /// <summary>
  ///   Delay in milliseconds after an error
  /// </summary>
  public int MsAfterRetry { get; set; } = 500;
}
