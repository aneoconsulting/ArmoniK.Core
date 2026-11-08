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


using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Hosting;

namespace ArmoniK.Core.Common.Pollster;

/// <summary>
///   Hosted service that runs the main loop of the <see cref="Pollster" />
/// </summary>
public class PollsterWorker : BackgroundService
{
  private readonly Pollster pollster_;

  /// <summary>
  ///   Creates the hosted service of the given pollster
  /// </summary>
  /// <param name="pollster">Pollster to run</param>
  public PollsterWorker(Pollster pollster)
    => pollster_ = pollster;

  /// <inheritdoc />
  protected override Task ExecuteAsync(CancellationToken stoppingToken)
    => pollster_.MainLoop();
}
