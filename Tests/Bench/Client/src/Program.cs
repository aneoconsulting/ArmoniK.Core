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
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Samples.Bench.Client.Options;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Serilog;
using Serilog.Formatting.Compact;

namespace ArmoniK.Samples.Bench.Client;

internal static class Program
{
  private static async Task Main()
  {
    var builder       = new ConfigurationBuilder().AddEnvironmentVariables();
    var configuration = builder.Build();
    Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(configuration)
                                          .Enrich.FromLogContext()
                                          .WriteTo.Console(new CompactJsonFormatter())
                                          .CreateBootstrapLogger();

    var factory = new LoggerFactory().AddSerilog();

    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>();
    var optionsHtcMock = new BenchOptions();
    configuration.GetSection(BenchOptions.SettingSection)
                 .Bind(optionsHtcMock);
    var channel = GrpcChannelFactory.CreateChannel(options);

    var submitterClient = new Submitter.SubmitterClient(channel);

    var createSessionRequest = new CreateSessionRequest
                               {
                                 DefaultTaskOption = new TaskOptions
                                                     {
                                                       MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                                                       MaxRetries  = 2,
                                                       Priority    = 1,
                                                       PartitionId = optionsHtcMock.Partition,
                                                       Options =
                                                       {
                                                         {
                                                           "TaskDurationMs", optionsHtcMock.TaskDurationMs.ToString()
                                                         },
                                                         {
                                                           "TaskError", optionsHtcMock.TaskError
                                                         },
                                                         {
                                                           "TaskRpcException", optionsHtcMock.TaskRpcException
                                                         },
                                                       },
                                                     },
                                 PartitionIds =
                                 {
                                   optionsHtcMock.Partition,
                                 },
                               };
    var createSessionReply = submitterClient.CreateSession(createSessionRequest);

    var createTaskReply = await submitterClient.CreateTasksAsync(createSessionReply.SessionId,
                                                                 null,
                                                                 Enumerable.Range(0,
                                                                                  optionsHtcMock.NTasks)
                                                                           .Select(i => new TaskRequest
                                                                                        {
                                                                                          ExpectedOutputKeys =
                                                                                          {
                                                                                            Guid.NewGuid() + "root" + i,
                                                                                          },
                                                                                          Payload = UnsafeByteOperations.UnsafeWrap(BitConverter.GetBytes(i)),
                                                                                        }))
                                               .ConfigureAwait(false);

    var sessionClient = new SessionClient(submitterClient,
                                          createSessionReply.SessionId,
                                          NullLogger<SessionClient>.Instance);

    foreach (var creationStatus in createTaskReply.CreationStatusList.CreationStatuses)
    {
      var result = sessionClient.GetResult(creationStatus.TaskInfo.TaskId);
    }

    Console.CancelKeyPress += (sender,
                               args) =>
                              {
                                args.Cancel = true;
                                submitterClient.CancelSession(new Session
                                                              {
                                                                Id = createSessionReply.SessionId,
                                                              });
                                Environment.Exit(0);
                              };
  }
}
