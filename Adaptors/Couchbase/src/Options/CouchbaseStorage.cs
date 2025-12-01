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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

namespace ArmoniK.Core.Adapters.Couchbase.Options
{
  /// <summary>
  ///   Represents the configuration settings for Couchbase object storage.
  /// </summary>
  public class CouchbaseStorage
  {
    /// <summary>
    ///   The name of the configuration section for CouchbaseStorage settings.
    /// </summary>
    public const string SettingSection = "CouchbaseStorage";
    
    /// <summary>
    ///   Name of the Couchbase bucket to use for object storage. Default is "_default".
    /// </summary>
    public string BucketName { get; init; } = "_default";
    
    /// <summary>
    ///   Name of the scope within the bucket. Default is "_default".
    /// </summary>
    public string ScopeName { get; init; } = "_default";
    
    /// <summary>
    ///   Name of the collection within the scope. Default is "_default".
    /// </summary>
    public string CollectionName { get; init; } = "_default";
  }
}
