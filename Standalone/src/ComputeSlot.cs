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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

using ArmoniK.Api.Common.Options;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Injection;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Utils;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

using PollsterOptions = ArmoniK.Core.Common.Injection.Options.Pollster;

namespace ArmoniK.Core.Standalone;

/// <summary>
///   Builds the host of a compute slot: one polling agent, connected to one external worker.
/// </summary>
/// <remarks>
///   Each slot has its own host, so that the singletons of the compute plane (pollster, agent, worker connection) are
///   not shared between slots. The storage (tables, queue, object storage) is shared: the instances of the root host are
///   injected in each slot.
/// </remarks>
internal static class ComputeSlot
{
  /// <summary>
  ///   Maximal length of a Unix domain socket path supported on Linux and macOS
  /// </summary>
  private const int MaxSocketPathLength = 103;

  /// <summary>
  ///   Expand the placeholders of an address template
  /// </summary>
  /// <param name="template">Address template, with <c>{data}</c> and <c>{slot}</c> placeholders</param>
  /// <param name="dataFolder">Absolute path of the data folder</param>
  /// <param name="slot">Index of the slot</param>
  /// <returns>
  ///   The address of the slot
  /// </returns>
  public static string Expand(string template,
                              string dataFolder,
                              int    slot)
    => template.Replace("{data}",
                        dataFolder)
               .Replace("{slot}",
                        slot.ToString());

  /// <summary>
  ///   Configuration specific to a slot: the addresses of its worker and agent sockets, and its cache folders
  /// </summary>
  /// <param name="options">Options of the standalone host</param>
  /// <param name="dataFolder">Absolute path of the data folder</param>
  /// <param name="slot">Index of the slot</param>
  /// <returns>
  ///   The configuration entries of the slot
  /// </returns>
  public static Dictionary<string, string?> SlotConfiguration(StandaloneOptions options,
                                                              string            dataFolder,
                                                              int               slot)
  {
    var slotFolder = Path.Combine(dataFolder,
                                  $"slot{slot}");

    return new Dictionary<string, string?>
           {
             [$"{ComputePlane.SettingSection}:{ComputePlane.WorkerChannelSection}:Address"] = Expand(options.WorkerAddress,
                                                                                                     dataFolder,
                                                                                                     slot),
             [$"{ComputePlane.SettingSection}:{ComputePlane.WorkerChannelSection}:SocketType"] = options.SocketType.ToString(),
             [$"{ComputePlane.SettingSection}:{ComputePlane.AgentChannelSection}:Address"] = Expand(options.AgentAddress,
                                                                                                    dataFolder,
                                                                                                    slot),
             [$"{ComputePlane.SettingSection}:{ComputePlane.AgentChannelSection}:SocketType"] = options.SocketType.ToString(),
             [$"{PollsterOptions.SettingSection}:{nameof(PollsterOptions.SharedCacheFolder)}"] = Path.Combine(slotFolder,
                                                                                                              "shared"),
             [$"{PollsterOptions.SettingSection}:{nameof(PollsterOptions.InternalCacheFolder)}"] = Path.Combine(slotFolder,
                                                                                                                "internal"),
           };
  }

  /// <summary>
  ///   Build the host of a compute slot
  /// </summary>
  /// <param name="slot">Index of the slot</param>
  /// <param name="options">Options of the standalone host</param>
  /// <param name="dataFolder">Absolute path of the data folder</param>
  /// <param name="rootConfiguration">Configuration of the standalone host</param>
  /// <param name="root">Services of the root host, holding the shared storage</param>
  /// <returns>
  ///   The host of the slot, not started, and the identifier of its agent
  /// </returns>
  public static (IHost host, AgentIdentifier identifier) Build(int               slot,
                                                               StandaloneOptions options,
                                                               string            dataFolder,
                                                               IConfiguration    rootConfiguration,
                                                               IServiceProvider  root)
  {
    var slotConfiguration = SlotConfiguration(options,
                                              dataFolder,
                                              slot);

    // The folders of the slot must exist before the agent creates its socket and the pollster its caches
    foreach (var key in new[]
                        {
                          $"{ComputePlane.SettingSection}:{ComputePlane.AgentChannelSection}:Address",
                          $"{ComputePlane.SettingSection}:{ComputePlane.WorkerChannelSection}:Address",
                        })
    {
      if (options.SocketType == GrpcSocketType.UnixDomainSocket)
      {
        var path = slotConfiguration[key]!;

        // Unix domain socket paths are limited to about 100 characters (108 bytes on Linux, 104 on macOS)
        if (path.Length > MaxSocketPathLength)
        {
          throw new InvalidOperationException($"The socket path '{path}' of slot {slot} is longer than {MaxSocketPathLength} characters, " +
                                              $"which Unix domain sockets do not support. Use a shorter {StandaloneOptions.SettingSection}:{nameof(StandaloneOptions.DataFolder)}, " +
                                              $"or set {StandaloneOptions.SettingSection}:{nameof(StandaloneOptions.WorkerAddress)} and " +
                                              $"{StandaloneOptions.SettingSection}:{nameof(StandaloneOptions.AgentAddress)} to shorter paths.");
        }

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
      }
    }

    Directory.CreateDirectory(slotConfiguration[$"{PollsterOptions.SettingSection}:{nameof(PollsterOptions.SharedCacheFolder)}"]!);
    Directory.CreateDirectory(slotConfiguration[$"{PollsterOptions.SettingSection}:{nameof(PollsterOptions.InternalCacheFolder)}"]!);

    var configuration = new ConfigurationManager();
    configuration.AddConfiguration(rootConfiguration);
    configuration.AddInMemoryCollection(slotConfiguration);

    var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings
                                                     {
                                                       Configuration = configuration,
                                                     });

    var pollsterOptions = configuration.GetSection(PollsterOptions.SettingSection)
                                       .Get<PollsterOptions>() ?? new PollsterOptions();

    var identifier = new AgentIdentifier
                     {
                       OwnerPodId   = $"standalone-slot{slot}",
                       OwnerPodName = $"{Environment.MachineName}-slot{slot}",
                     };

    var loggerInit = root.GetRequiredService<LoggerInit>();

    builder.Services.Configure<HostOptions>(hostOptions => hostOptions.ShutdownTimeout = 2 * pollsterOptions.GraceDelay)
           .AddLogging(loggerInit.Configure)
           .AddSingleton(loggerInit)
           .AddSingleton(root.GetRequiredService<ActivitySource>())
           .AddSingleton(root.GetRequiredService<ITaskTable>())
           .AddSingleton(root.GetRequiredService<ISessionTable>())
           .AddSingleton(root.GetRequiredService<IResultTable>())
           .AddSingleton(root.GetRequiredService<IPartitionTable>())
           .AddSingleton(root.GetRequiredService<IPushQueueStorage>())
           .AddSingleton(root.GetRequiredService<IPullQueueStorage>())
           .AddSingleton(root.GetRequiredService<IObjectStorage>())
           .AddArmoniKComputePlane(configuration,
                                   pollsterOptions)
           // Registered after the compute plane, so that they override its defaults
           .AddSingleton(identifier)
           .AddSingleton<ITaskProcessingChecker>(root.GetRequiredService<InProcessAgents>());

    return (builder.Build(), identifier);
  }
}
