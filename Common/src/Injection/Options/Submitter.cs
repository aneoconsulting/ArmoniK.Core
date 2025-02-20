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

namespace ArmoniK.Core.Common.Injection.Options;

/// <summary>
///   Configuration for <see cref="gRPC.Services.Submitter" />.
/// </summary>
public class Submitter
{
  /// <summary>
  ///   Path to the section containing the values in the configuration object
  /// </summary>
  public const string SettingSection = nameof(Submitter);

  /// <summary>
  ///   Name of the default partition in which submit tasks
  /// </summary>
  public string DefaultPartition { get; set; } = string.Empty;

  /// <summary>
  ///   Specify the maximum number of errors a submitter can encounter before being considered unhealthy
  ///   Negative values disable the check
  /// </summary>
  public int MaxErrorAllowed { get; set; } = 5;


  /// <summary>
  ///   Toggle payload suppression after the task is successful
  ///   default: false
  /// </summary>
  public bool DeletePayload { get; set; } = false;

  /// <summary>
  ///   Parallelism used in the control plane when possible. Defaults to the number of threads.
  /// </summary>
  public int DegreeOfParallelism { get; set; } = 0;
}
