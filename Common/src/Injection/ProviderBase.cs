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
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Threading.Tasks;

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace ArmoniK.Core.Common.Injection;

/// <summary>
///   Implement a mechanism to initialize a class during its first access
/// </summary>
/// <typeparam name="T">Type of the class to initialize</typeparam>
public abstract class ProviderBase<T> : IHealthCheckProvider
{
  private readonly Func<Task<T>> builder_;
  private readonly object        lockObj_ = new();
  private          T?            object_;

  /// <summary>
  ///   Constructor of the provider with a method to initialize the class that will be provided.
  /// </summary>
  /// <param name="builder">Method to initialize the class that will be provided</param>
  protected ProviderBase(Func<Task<T>> builder)
    => builder_ = builder;

  /// <inheritdoc />
  public Task<HealthCheckResult> Check(HealthCheckTag tag)
    => Task.FromResult(object_ is not null
                         ? HealthCheckResult.Healthy()
                         : HealthCheckResult.Unhealthy());

  /// <summary>
  ///   Getter for the class to initialize.
  ///   If the class is initialized, returns the class.
  ///   If not, initializes the class then returns it.
  /// </summary>
  /// <returns>
  ///   The initialized class
  /// </returns>
  public T Get()
  {
    // Double null check to avoid the lock once initialization is finished
    if (object_ is not null)
    {
      return object_;
    }

    lock (lockObj_)
    {
      // can be simplified with Resharper :)
      object_ = object_ is null
                  ? builder_()
                    .Result
                  : object_;
    }

    return object_;
  }
}
