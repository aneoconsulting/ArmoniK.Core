// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;

using ArmoniK.Api.Client.Options;
using ArmoniK.Api.Client.Submitter;
using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

[TestFixture]
[Obsolete]
internal class StreamWrapperTests
{
  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string?> baseConfig = new()
                                             {
                                               {
                                                 "GrpcClient:Endpoint", "http://localhost:5001"
                                               },
                                               {
                                                 "Partition", "TestPartition"
                                               },
                                             };

    var builder = new ConfigurationBuilder().AddInMemoryCollection(baseConfig)
                                            .AddEnvironmentVariables();
    var configuration = builder.Build();
    var options = configuration.GetRequiredSection(GrpcClient.SettingSection)
                               .Get<GrpcClient>()!;

    partition_ = configuration.GetValue<string>("Partition");

    Console.WriteLine($"endpoint : {options.Endpoint}");
    channel_      = GrpcChannelFactory.CreateChannel(options);
    client_       = new Submitter.SubmitterClient(channel_);
    resultClient_ = new Results.ResultsClient(channel_);
    Console.WriteLine("Client created");
  }

  [TearDown]
  public void TearDown()
  {
    partition_    = null;
    client_       = null;
    resultClient_ = null;
    channel_?.ShutdownAsync()
            .Wait();
    channel_ = null;
  }

  private Submitter.SubmitterClient? client_;
  private string?                    partition_;
  private ChannelBase?               channel_;
  private Results.ResultsClient?     resultClient_;

  [TestCase(2,
            ExpectedResult = 4)]
  [TestCase(4,
            ExpectedResult = 16)]
  public async Task<int> Square(int input)
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var result = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                 {
                                                                   SessionId = sessionId,
                                                                   Results =
                                                                   {
                                                                     new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                     {
                                                                       Name = "exp",
                                                                     },
                                                                   },
                                                                 })
                                     .ConfigureAwait(false);

    var expectedOutput = result.Results.Single()
                               .ResultId;

    var payload = new TestPayload
                  {
                    Type      = TestPayload.TaskType.Compute,
                    DataBytes = BitConverter.GetBytes(input),
                    ResultKey = expectedOutput,
                  };

    var req = new TaskRequest
              {
                Payload = ByteString.CopyFrom(payload.Serialize()),
                ExpectedOutputKeys =
                {
                  expectedOutput,
                },
              };

    Console.WriteLine("TaskRequest Created");

    await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                 null,
                                                 new[]
                                                 {
                                                   req,
                                                 })
                  .ConfigureAwait(false);

    var resultRequest = new ResultRequest
                        {
                          ResultId = expectedOutput,
                          Session  = sessionId,
                        };

#pragma warning disable CS0612 // Type or member is obsolete
    var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete

    Assert.AreEqual(availabilityReply.TypeCase,
                    AvailabilityReply.TypeOneofCase.Ok);

    client_.TryGetResultStream(resultRequest);

    var resultPayload = await channel_!.GetPayloadAsyncAndCheck(sessionId,
                                                                expectedOutput)
                                       .ConfigureAwait(false);

    Console.WriteLine($"Payload Type : {resultPayload.Type} - {expectedOutput}");
    if (resultPayload.Type == TestPayload.TaskType.Result)
    {
      var output = BitConverter.ToInt32(resultPayload.DataBytes);
      Console.WriteLine($"Result : {output}");
      return output;
    }

    return 0;
  }

  [Test(ExpectedResult = Output.TypeOneofCase.Error)]
  [Repeat(2)]
  public async Task<Output.TypeOneofCase> TaskError()
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var result = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                 {
                                                                   SessionId = sessionId,
                                                                   Results =
                                                                   {
                                                                     new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                     {
                                                                       Name = "exp",
                                                                     },
                                                                   },
                                                                 })
                                     .ConfigureAwait(false);

    var expectedOutput = result.Results.Single()
                               .ResultId;

    var payload = new TestPayload
                  {
                    Type = TestPayload.TaskType.Error,
                  };

    var req = new TaskRequest
              {
                Payload = ByteString.CopyFrom(payload.Serialize()),
                ExpectedOutputKeys =
                {
                  expectedOutput,
                },
              };

    Console.WriteLine("TaskRequest Created");

    var taskIds = await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                               null,
                                                               new[]
                                                               {
                                                                 req,
                                                               })
                                .ConfigureAwait(false);

    var resultRequest = new TaskOutputRequest
                        {
                          TaskId  = taskIds.Single(),
                          Session = sessionId,
                        };

    var taskOutput = client_!.TryGetTaskOutput(resultRequest);
    Console.WriteLine(taskOutput.ToString());
    return taskOutput.TypeCase;
  }

  [Test]
  [Repeat(2)]
  public async Task TaskFailed()
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var payload = new TestPayload
                  {
                    Type = TestPayload.TaskType.ReturnFailed,
                  };

    var results = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                  {
                                                                    SessionId = sessionId,
                                                                    Results =
                                                                    {
                                                                      Enumerable.Range(0,
                                                                                       10)
                                                                                .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                             {
                                                                                               Name = $"myresult{i}",
                                                                                             }),
                                                                    },
                                                                  })
                                      .ConfigureAwait(false);

    var taskRequests = new List<TaskRequest>();

    for (var i = 0; i < 10; i++)
    {
      var req = new TaskRequest
                {
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    results.Results[i]
                           .ResultId,
                  },
                };
      taskRequests.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    var taskIds = await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                               null,
                                                               taskRequests)
                                .ConfigureAwait(false);

    var taskOutput = taskIds.Select(id =>
                                    {
                                      var resultRequest = new TaskOutputRequest
                                                          {
                                                            TaskId  = id,
                                                            Session = sessionId,
                                                          };

                                      var taskOutput = client_!.TryGetTaskOutput(resultRequest);
                                      Console.WriteLine(id + " - " + taskOutput);
                                      return taskOutput.TypeCase;
                                    });

    Assert.IsTrue(taskOutput.All(status => status == Output.TypeOneofCase.Error));
  }


  [Test]
  public async Task MultipleTasks([Values(4,
                                          5,
                                          100)]
                                  int n,
                                  [Values(TestPayload.TaskType.Compute,
                                          TestPayload.TaskType.Transfer)]
                                  TestPayload.TaskType taskType)
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var results = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                  {
                                                                    SessionId = sessionId,
                                                                    Results =
                                                                    {
                                                                      Enumerable.Range(0,
                                                                                       n)
                                                                                .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                             {
                                                                                               Name = $"{nameof(MultipleTasks)}myresult{i}",
                                                                                             }),
                                                                    },
                                                                  })
                                      .ConfigureAwait(false);

    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var payload = new TestPayload
                    {
                      Type      = taskType,
                      DataBytes = BitConverter.GetBytes(i),
                      ResultKey = results.Results[i]
                                         .ResultId,
                    };

      var req = new TaskRequest
                {
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    results.Results[i]
                           .ResultId,
                  },
                };
      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                 null,
                                                 taskRequestList)
                  .ConfigureAwait(false);

    var resultAvailability = taskRequestList.Select(request =>
                                                    {
                                                      var resultRequest = new ResultRequest
                                                                          {
                                                                            ResultId = request.ExpectedOutputKeys.Single(),
                                                                            Session  = sessionId,
                                                                          };
#pragma warning disable CS0612 // Type or member is obsolete
                                                      var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete
                                                      return availabilityReply.TypeCase;
                                                    });

    Assert.IsTrue(resultAvailability.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultList = taskRequestList.Select(async request =>
                                            {
                                              var resultPayload = await channel_!.GetPayloadAsyncAndCheck(sessionId,
                                                                                                          request.ExpectedOutputKeys.Single())
                                                                                 .ConfigureAwait(false);

                                              Console.WriteLine($"Payload Type : {resultPayload.Type} - {request.ExpectedOutputKeys.Single()}");
                                              if (resultPayload.Type == TestPayload.TaskType.Result)
                                              {
                                                var output = BitConverter.ToInt32(resultPayload.DataBytes);
                                                Console.WriteLine($"Result : {output}");
                                                return output;
                                              }

                                              return 0;
                                            });

    var sum = resultList.Aggregate((t1,
                                    t2) => Task.FromResult(t1.Result + t2.Result));
    Assert.AreEqual(n * (n - 1) * (2 * n - 1) / 6,
                    sum.Result);
  }

  [Test]
  public async Task MultipleDataDependencies([Values(1,
                                                     5,
                                                     20)]
                                             int n)
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var resultsMetaData = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                          {
                                                                            SessionId = sessionId,
                                                                            Results =
                                                                            {
                                                                              Enumerable.Range(0,
                                                                                               2 * n)
                                                                                        .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                                     {
                                                                                                       Name = $"{nameof(MultipleDataDependencies)}myresult{i}",
                                                                                                     }),
                                                                            },
                                                                          })
                                              .ConfigureAwait(false);

    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var payload = new TestPayload
                    {
                      Type      = TestPayload.TaskType.DatadepTransfer,
                      DataBytes = BitConverter.GetBytes(i + 5),
                      ResultKey = resultsMetaData.Results[2 * i]
                                                 .ResultId,
                      ResultKey2 = resultsMetaData.Results[2 * i + 1]
                                                  .ResultId,
                    };

      var req = new TaskRequest
                {
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    resultsMetaData.Results[2 * i]
                                   .ResultId,
                    resultsMetaData.Results[2 * i + 1]
                                   .ResultId,
                  },
                };
      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                 null,
                                                 taskRequestList)
                  .ConfigureAwait(false);

    var resultAvailability1 = taskRequestList.Select(request =>
                                                     {
                                                       var resultRequest = new ResultRequest
                                                                           {
                                                                             ResultId = request.ExpectedOutputKeys.First(),
                                                                             Session  = sessionId,
                                                                           };
#pragma warning disable CS0612 // Type or member is obsolete
                                                       var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete
                                                       return availabilityReply.TypeCase;
                                                     });

    Assert.IsTrue(resultAvailability1.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultAvailability2 = taskRequestList.Select(request =>
                                                     {
                                                       var resultRequest = new ResultRequest
                                                                           {
                                                                             ResultId = request.ExpectedOutputKeys.Last(),
                                                                             Session  = sessionId,
                                                                           };
#pragma warning disable CS0612 // Type or member is obsolete
                                                       var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete
                                                       return availabilityReply.TypeCase;
                                                     });

    Assert.IsTrue(resultAvailability2.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var results = taskRequestList.Select(async request =>
                                         {
                                           var resultPayload1 = await channel_!.GetPayloadAsyncAndCheck(sessionId,
                                                                                                        request.ExpectedOutputKeys.First())
                                                                               .ConfigureAwait(false);

                                           var resultPayload2 = await channel_!.GetPayloadAsyncAndCheck(sessionId,
                                                                                                        request.ExpectedOutputKeys.Last())
                                                                               .ConfigureAwait(false);

                                           var resultInt1 = BitConverter.ToInt32(resultPayload1.DataBytes);
                                           var resultInt2 = BitConverter.ToInt32(resultPayload2.DataBytes);

                                           Console.WriteLine($"Result1 {resultInt1}, Result2 {resultInt2}");

                                           return 2 * resultInt2 == resultInt1;
                                         });
    Assert.IsTrue(results.All(task => task.Result));
  }


  [Test]
  public async Task LargePayloads([Values(2,
                                          10)]
                                  int n,
                                  [Values(1,
                                          2,
                                          5,
                                          10)]
                                  int size)
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var taskRequestList = new List<TaskRequest>();

    var rnd       = new Random();
    var dataBytes = new byte[size * 1024 * 128];
    rnd.NextBytes(dataBytes);
    var hash = SHA256.HashData(dataBytes);

    var payload = new TestPayload
                  {
                    Type      = TestPayload.TaskType.PayloadCheckSum,
                    DataBytes = dataBytes,
                  };
    var serializedPayload = payload.Serialize();
    var byteString        = ByteString.CopyFrom(serializedPayload);
    Console.WriteLine("Payload Hash " + Convert.ToBase64String(SHA256.HashData(serializedPayload)));
    Console.WriteLine($"Payload size {serializedPayload.Length}");

    var resultsMetaData = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                          {
                                                                            SessionId = sessionId,
                                                                            Results =
                                                                            {
                                                                              Enumerable.Range(0,
                                                                                               n)
                                                                                        .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                                     {
                                                                                                       Name = $"{nameof(LargePayloads)}myresult{i}",
                                                                                                     }),
                                                                            },
                                                                          })
                                              .ConfigureAwait(false);

    for (var i = 0; i < n; i++)
    {
      var req = new TaskRequest
                {
                  Payload = byteString,
                  ExpectedOutputKeys =
                  {
                    resultsMetaData.Results[i]
                                   .ResultId,
                  },
                };

      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                 null,
                                                 taskRequestList)
                  .ConfigureAwait(false);

    var resultAvailability = taskRequestList.Select(request =>
                                                    {
                                                      var resultRequest = new ResultRequest
                                                                          {
                                                                            ResultId = request.ExpectedOutputKeys.Single(),
                                                                            Session  = sessionId,
                                                                          };
#pragma warning disable CS0612 // Type or member is obsolete
                                                      var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete
                                                      return availabilityReply.TypeCase;
                                                    });

    Assert.IsTrue(resultAvailability.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultList = taskRequestList.Select(async request =>
                                            {
                                              var resultPayload = await channel_!.GetPayloadAsyncAndCheck(sessionId,
                                                                                                          request.ExpectedOutputKeys.Single())
                                                                                 .ConfigureAwait(false);

                                              Console.WriteLine($"Payload Type : {resultPayload.Type} - {request.ExpectedOutputKeys.Single()}");
                                              if (resultPayload.Type == TestPayload.TaskType.Result)
                                              {
                                                return hash.SequenceEqual(resultPayload.DataBytes ?? Array.Empty<byte>());
                                              }

                                              return false;
                                            });

    Assert.IsTrue(resultList.All(_ => true));
  }

  [Test]
  public async Task EmptyPayload()
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var result = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                 {
                                                                   SessionId = sessionId,
                                                                   Results =
                                                                   {
                                                                     new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                     {
                                                                       Name = nameof(EmptyPayload),
                                                                     },
                                                                   },
                                                                 })
                                     .ConfigureAwait(false);

    var outputId = result.Results.Single()
                         .ResultId;

    var taskIds = await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                               null,
                                                               new[]
                                                               {
                                                                 new TaskRequest
                                                                 {
                                                                   ExpectedOutputKeys =
                                                                   {
                                                                     outputId,
                                                                   },
                                                                   Payload = ByteString.Empty,
                                                                 },
                                                               })
                                .ConfigureAwait(false);

    var resultRequest = new ResultRequest
                        {
                          ResultId = outputId,
                          Session  = sessionId,
                        };
#pragma warning disable CS0612 // Type or member is obsolete
    var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete

    Assert.AreEqual(AvailabilityReply.TypeOneofCase.Error,
                    availabilityReply.TypeCase);

    var taskOutput = client_.TryGetTaskOutput(new TaskOutputRequest
                                              {
                                                TaskId  = taskIds.Single(),
                                                Session = sessionId,
                                              });
    Console.WriteLine(outputId + " - " + taskOutput);

    Assert.AreEqual(Output.TypeOneofCase.Error,
                    taskOutput.TypeCase);
  }

  [Test]
  public async Task PriorityShouldHaveAnEffect([Values(10,
                                                       50)]
                                               int n)
  {
    var sessionId = client_!.CreateSessionAndCheckReply(partition_!);

    var tasks = Enumerable.Range(1,
                                 9)
                          .Select(i => Task.Run(() => RunForPriority(sessionId,
                                                                     i,
                                                                     n)))
                          .ToList();

    foreach (var task in tasks)
    {
      await task.ConfigureAwait(false);
    }
  }

  private async Task<long> RunForPriority(string sessionId,
                                          int    priority,
                                          int    n)
  {
    Console.WriteLine("Launch taks with priority " + priority);
    var sw = Stopwatch.StartNew();
    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                        MaxRetries  = 3,
                        Priority    = priority,
                        PartitionId = partition_,
                      };
    var taskRequestList = new List<TaskRequest>();

    var resultsMetaData = await resultClient_!.CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest
                                                                          {
                                                                            SessionId = sessionId,
                                                                            Results =
                                                                            {
                                                                              Enumerable.Range(0,
                                                                                               n)
                                                                                        .Select(i => new CreateResultsMetaDataRequest.Types.ResultCreate
                                                                                                     {
                                                                                                       Name = $"{nameof(RunForPriority)}myresult{i}",
                                                                                                     }),
                                                                            },
                                                                          })
                                              .ConfigureAwait(false);

    for (var i = 0; i < n; i++)
    {
      var payload = new TestPayload
                    {
                      Type      = TestPayload.TaskType.Compute,
                      DataBytes = BitConverter.GetBytes(1),
                      ResultKey = resultsMetaData.Results[i]
                                                 .ResultId,
                    };

      var req = new TaskRequest
                {
                  Payload = ByteString.CopyFrom(payload.Serialize()),
                  ExpectedOutputKeys =
                  {
                    resultsMetaData.Results[i]
                                   .ResultId,
                  },
                };

      taskRequestList.Add(req);
    }

    await client_!.CreateTasksAndCheckReplyAsync(sessionId,
                                                 taskOptions,
                                                 taskRequestList)
                  .ConfigureAwait(false);

    var resultAvailability = taskRequestList.Select(request =>
                                                    {
                                                      var resultRequest = new ResultRequest
                                                                          {
                                                                            ResultId = request.ExpectedOutputKeys.Single(),
                                                                            Session  = sessionId,
                                                                          };
#pragma warning disable CS0612 // Type or member is obsolete
                                                      var availabilityReply = client_!.WaitForAvailability(resultRequest);
#pragma warning restore CS0612 // Type or member is obsolete
                                                      return availabilityReply.TypeCase;
                                                    });

    if (resultAvailability.Any(c => c != AvailabilityReply.TypeOneofCase.Ok))
    {
      return -1;
    }

    var resultList = taskRequestList.Select(async request =>
                                            {
                                              var resultPayload = await channel_!.GetPayloadAsyncAndCheck(sessionId,
                                                                                                          request.ExpectedOutputKeys.Single())
                                                                                 .ConfigureAwait(false);
                                              return resultPayload is
                                                     {
                                                       Type: TestPayload.TaskType.Result,
                                                     };
                                            });
    Console.WriteLine("Executed taks with priority " + priority + " in " + sw.ElapsedMilliseconds);
    return resultList.All(task => task.Result)
             ? sw.ElapsedMilliseconds
             : 0;
  }
}
