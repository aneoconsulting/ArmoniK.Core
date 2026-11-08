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

using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Common.Pollster;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace ArmoniK.Core.Standalone;

/// <summary>
///   Runs a compute slot, and restarts it when it stops.
/// </summary>
/// <remarks>
///   <para>
///     A polling agent stops for good when its worker is unreachable, for instance when the worker is not started yet or
///     has crashed; in a deployment, the orchestrator then restarts the agent. This supervisor plays that role: it waits
///     until the worker of the slot accepts connections, starts the slot, and builds and starts a new slot when the
///     previous one stops.
///   </para>
/// </remarks>
internal sealed class ComputeSlotSupervisor : BackgroundService
{
  private static readonly TimeSpan RetryDelay   = TimeSpan.FromSeconds(1);
  private static readonly TimeSpan RestartDelay = TimeSpan.FromSeconds(5);

  private readonly InProcessAgents   checker_;
  private readonly string            dataFolder_;
  private readonly ILogger           logger_;
  private readonly StandaloneOptions options_;
  private readonly IServiceProvider  root_;
  private readonly IConfiguration    rootConfiguration_;
  private readonly int               slot_;

  /// <summary>
  ///   Creates the supervisor of a compute slot
  /// </summary>
  /// <param name="slot">Index of the slot</param>
  /// <param name="options">Options of the standalone host</param>
  /// <param name="dataFolder">Absolute path of the data folder</param>
  /// <param name="rootConfiguration">Configuration of the standalone host</param>
  /// <param name="root">Services of the root host, holding the shared storage</param>
  public ComputeSlotSupervisor(int               slot,
                               StandaloneOptions options,
                               string            dataFolder,
                               IConfiguration    rootConfiguration,
                               IServiceProvider  root)
  {
    slot_              = slot;
    options_           = options;
    dataFolder_        = dataFolder;
    rootConfiguration_ = rootConfiguration;
    root_              = root;
    checker_           = root.GetRequiredService<InProcessAgents>();
    logger_ = root.GetRequiredService<ILoggerFactory>()
                  .CreateLogger($"{typeof(ComputeSlotSupervisor).FullName}.Slot{slot}");
  }

  /// <inheritdoc />
  protected override async Task ExecuteAsync(CancellationToken stoppingToken)
  {
    var workerAddress = ComputeSlot.Expand(options_.WorkerAddress,
                                           dataFolder_,
                                           slot_);
    var agentAddress = ComputeSlot.Expand(options_.AgentAddress,
                                          dataFolder_,
                                          slot_);

    while (!stoppingToken.IsCancellationRequested)
    {
      var (host, identifier) = ComputeSlot.Build(slot_,
                                                 options_,
                                                 dataFolder_,
                                                 rootConfiguration_,
                                                 root_);
      try
      {
        // Creating the pollster starts the agent server, to which the worker connects
        checker_.Register(identifier.OwnerPodId,
                          host.Services.GetRequiredService<Pollster>());

        logger_.LogInformation("Compute slot {Slot} waits for its worker, which must listen on {WorkerAddress} and connect to the agent on {AgentAddress}",
                               slot_,
                               workerAddress,
                               agentAddress);

        await WaitForWorker(workerAddress,
                            stoppingToken)
          .ConfigureAwait(false);

        await host.StartAsync(stoppingToken)
                  .ConfigureAwait(false);

        logger_.LogInformation("Compute slot {Slot} started",
                               slot_);

        // Run until the root host stops, or until the slot stops by itself (for instance when its worker is lost)
        var lifetime = host.Services.GetRequiredService<IHostApplicationLifetime>();
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken,
                                                                            lifetime.ApplicationStopping);
        await Task.Delay(Timeout.Infinite,
                         stopped.Token)
                  .ConfigureAwait(false);
      }
      catch (OperationCanceledException)
      {
        // Stopping
      }
      catch (Exception e)
      {
        logger_.LogError(e,
                         "Compute slot {Slot} failed",
                         slot_);
      }
      finally
      {
        await host.StopAsync(CancellationToken.None)
                  .ConfigureAwait(false);
        if (host is IAsyncDisposable asyncDisposable)
        {
          await asyncDisposable.DisposeAsync()
                               .ConfigureAwait(false);
        }
        else
        {
          host.Dispose();
        }
      }

      if (stoppingToken.IsCancellationRequested)
      {
        break;
      }

      logger_.LogWarning("Compute slot {Slot} stopped, restarting it in {Delay}",
                         slot_,
                         RestartDelay);
      try
      {
        await Task.Delay(RestartDelay,
                         stoppingToken)
                  .ConfigureAwait(false);
      }
      catch (OperationCanceledException)
      {
        break;
      }
    }
  }

  /// <summary>
  ///   Wait until the worker accepts connections on its address
  /// </summary>
  /// <param name="workerAddress">Address of the worker</param>
  /// <param name="cancellationToken">Token used to cancel the wait</param>
  /// <returns>
  ///   Task completed when the worker accepts connections
  /// </returns>
  private async Task WaitForWorker(string            workerAddress,
                                   CancellationToken cancellationToken)
  {
    while (true)
    {
      try
      {
        if (options_.SocketType == GrpcSocketType.UnixDomainSocket)
        {
          using var socket = new Socket(AddressFamily.Unix,
                                        SocketType.Stream,
                                        ProtocolType.Unspecified);
          await socket.ConnectAsync(new UnixDomainSocketEndPoint(workerAddress),
                                    cancellationToken)
                      .ConfigureAwait(false);
        }
        else
        {
          var       uri    = new Uri(workerAddress);
          using var client = new TcpClient();
          await client.ConnectAsync(uri.Host,
                                    uri.Port,
                                    cancellationToken)
                      .ConfigureAwait(false);
        }

        return;
      }
      catch (SocketException)
      {
        await Task.Delay(RetryDelay,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
    }
  }
}
