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

namespace ArmoniK.Core.Adapters.S3.Options;

public class S3
{
  public const string SettingSection = nameof(S3);
  public       string EndpointUrl        { get; set; } = "";
  public       string Login              { get; set; } = "";
  public       string Password           { get; set; } = "";
  public       bool   MustForcePathStyle { get; set; }
  public       string BucketName         { get; set; } = "";

  /// <summary>
  ///   Number of tasks to be used in parallel execution
  /// </summary>
  public int DegreeOfParallelism { get; set; }

  /// <summary>
  ///   Size of one chunk when downloading an object by chunks
  /// </summary>
  public int ChunkDownloadSize { get; set; } = 65536;

  /// <summary>
  ///   Use Chunk Encoding during upload
  ///   Should be disabled for GCS
  /// </summary>
  public bool UseChunkEncoding { get; set; } = true;

  /// <summary>
  ///   If true, checksum will be verified by the client during upload
  /// </summary>
  public bool UseChecksum { get; set; } = true;

  /// <summary>
  ///   Number of retry in case of a connection error
  /// </summary>
  public int MaxRetry { get; set; } = 5;

  /// <summary>
  ///   Delay in milliseconds after an error
  /// </summary>
  public int MsAfterRetry { get; set; } = 500;

  /// <summary>
  ///   Get a copy of the options with confidential fields removed
  /// </summary>
  /// <returns>Confidential copy</returns>
  public S3 Confidential()
    => new()
       {
         BucketName          = BucketName,
         ChunkDownloadSize   = ChunkDownloadSize,
         DegreeOfParallelism = DegreeOfParallelism,
         EndpointUrl         = EndpointUrl,
         Login               = Login,
         MustForcePathStyle  = MustForcePathStyle,
         Password            = "[CONFIDENTIAL]",
         UseChecksum         = UseChecksum,
         UseChunkEncoding    = UseChunkEncoding,
       };
}
