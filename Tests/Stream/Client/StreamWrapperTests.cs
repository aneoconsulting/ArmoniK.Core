// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Stream.Client;
using ArmoniK.Extensions.Common.StreamWrapper.Tests.Common;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;
using Grpc.Net.Client;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

using TaskStatus = ArmoniK.Api.gRPC.V1.TaskStatus;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

[TestFixture]
internal class StreamWrapperTests
{
  [SetUp]
  public void SetUp()
  {
    Dictionary<string, string> baseConfig = new()
    {
      { "Grpc:Endpoint", "http://localhost:5001" },
    };

    var builder              = new ConfigurationBuilder().AddInMemoryCollection(baseConfig).AddEnvironmentVariables();
    var configuration        = builder.Build();
    var configurationSection = configuration.GetSection(Options.Grpc.SettingSection);
    var endpoint             = configurationSection.GetValue<string>("Endpoint");

    Console.WriteLine($"endpoint : {endpoint}");
    var channel = GrpcChannel.ForAddress(endpoint);
    client_ = new Submitter.SubmitterClient(channel);
    Console.WriteLine("Client created");
  }

  private Submitter.SubmitterClient client_;

  [TestCase(2,
            ExpectedResult = 4)]
  [TestCase(4,
            ExpectedResult = 16)]
  public async Task<int> Square(int input)
  {
    var sessionId = Guid.NewGuid() + "mytestsession";
    var taskId    = Guid.NewGuid() + "mytask";

    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
      MaxRetries  = 2,
      Priority    = 1,
    };

    Console.WriteLine("Creating Session");
    var session = client_.CreateSession(new CreateSessionRequest
    {
      DefaultTaskOption = taskOptions,
      Id                = sessionId,
    });
    switch (session.ResultCase)
    {
      case CreateSessionReply.ResultOneofCase.Error:
        throw new Exception("Error while creating session : " + session.Error);
      case CreateSessionReply.ResultOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateSessionReply.ResultOneofCase.Ok:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    Console.WriteLine("Session Created");

    var payload = new TestPayload
    {
      Type      = TestPayload.TaskType.Compute,
      DataBytes = BitConverter.GetBytes(input),
      ResultKey = taskId,
    };

    var req = new TaskRequest
    {
      Id      = taskId,
      Payload = ByteString.CopyFrom(payload.Serialize()),
      ExpectedOutputKeys =
      {
        taskId,
      },
    };

    Console.WriteLine("TaskRequest Created");

    var createTaskReply = await client_.CreateTasksAsync(sessionId,
                                                         taskOptions,
                                                         new[] { req });

    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        Console.WriteLine("Task Created");
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    var waitForCompletion = client_.WaitForCompletion(new WaitRequest
    {
      Filter = new TaskFilter
      {
        Session = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            sessionId,
          },
        },
        //Included = new TaskFilter.Types.StatusesRequest
        //{
        //  Statuses =
        //  {
        //    TaskStatus.Completed,
        //  },
        //},
      },
      StopOnFirstTaskCancellation = true,
      StopOnFirstTaskError        = true,
    });

    Console.WriteLine(waitForCompletion.ToString());
    
    var resultRequest = new ResultRequest
    {
      Key     = taskId,
      Session = sessionId,
    };

    var availabilityReply = client_.WaitForAvailability(resultRequest);

    Assert.AreEqual(availabilityReply.TypeCase, AvailabilityReply.TypeOneofCase.Ok);

    var streamingCall = client_.TryGetResultStream(resultRequest);

    var result = new List<byte>();

    var resultPayload = TestPayload.Deserialize(await client_.GetResultAsync(resultRequest));
    Console.WriteLine($"Payload Type : {resultPayload.Type}");
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
    var sessionId = Guid.NewGuid() + "mytestsession";
    var taskId    = Guid.NewGuid() + "mytask";

    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
      MaxRetries  = 2,
      Priority    = 1,
    };

    Console.WriteLine("Creating Session");
    var session = client_.CreateSession(new CreateSessionRequest
    {
      DefaultTaskOption = taskOptions,
      Id                = sessionId,
    });
    switch (session.ResultCase)
    {
      case CreateSessionReply.ResultOneofCase.Error:
        throw new Exception("Error while creating session : " + session.Error);
      case CreateSessionReply.ResultOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateSessionReply.ResultOneofCase.Ok:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    Console.WriteLine("Session Created");

    var payload = new TestPayload
    {
      Type = TestPayload.TaskType.Error,
    };

    var req = new TaskRequest
    {
      Id      = taskId,
      Payload = ByteString.CopyFrom(payload.Serialize()),
      ExpectedOutputKeys =
      {
        taskId,
      },
    };

    Console.WriteLine("TaskRequest Created");

    var createTaskReply = await client_.CreateTasksAsync(sessionId,
                                                         taskOptions,
                                                         new[] { req });

    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        Console.WriteLine("Task Created");
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    var waitForCompletion = client_.WaitForCompletion(new WaitRequest
    {
      Filter = new TaskFilter
      {
        Session = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            sessionId,
          },
        },
      },
      StopOnFirstTaskCancellation = true,
      StopOnFirstTaskError        = true,
    });

    Console.WriteLine(waitForCompletion.ToString());

    var resultRequest = new ResultRequest
    {
      Key     = taskId,
      Session = sessionId,
    };

    var taskOutput = client_.TryGetTaskOutput(resultRequest);
    Console.WriteLine(taskOutput.ToString());
    return taskOutput.TypeCase;
  }

  // TODO: should it be TaskStatus.Failed ?
  [Test(ExpectedResult = TaskStatus.Completed)]
  [Repeat(2)]
  public async Task<TaskStatus> TaskFailed()
  {
    var sessionId = Guid.NewGuid() + nameof(TaskFailed);
    var taskId = Guid.NewGuid() + "mytask";

    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
      MaxRetries = 2,
      Priority = 1,
    };

    Console.WriteLine("Creating Session");
    var session = client_.CreateSession(new CreateSessionRequest
    {
      DefaultTaskOption = taskOptions,
      Id = sessionId,
    });
    switch (session.ResultCase)
    {
      case CreateSessionReply.ResultOneofCase.Error:
        throw new Exception("Error while creating session : " + session.Error);
      case CreateSessionReply.ResultOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateSessionReply.ResultOneofCase.Ok:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    Console.WriteLine("Session Created");

    var payload = new TestPayload
    {
      Type = TestPayload.TaskType.ReturnFailed,
    };

    var req = new TaskRequest
    {
      Id = taskId,
      Payload = ByteString.CopyFrom(payload.Serialize()),
      ExpectedOutputKeys =
      {
        taskId,
      },
    };

    Console.WriteLine("TaskRequest Created");

    var createTaskReply = await client_.CreateTasksAsync(sessionId,
                                                         taskOptions,
                                                         new[] { req });

    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        Console.WriteLine("Task Created");
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    var waitForCompletion = client_.WaitForCompletion(new WaitRequest
    {
      Filter = new TaskFilter
      {
        Session = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            sessionId,
          },
        },
      },
      StopOnFirstTaskCancellation = true,
      StopOnFirstTaskError = true,
    });

    Console.WriteLine(waitForCompletion.ToString());

    var resultRequest = new ResultRequest
    {
      Key = taskId,
      Session = sessionId,
    };

    var taskOutput = client_.TryGetTaskOutput(resultRequest);
    Console.WriteLine(taskOutput.ToString());
    return taskOutput.Status;
  }



  [Test]
  public async Task MultipleTasks([Values(4,
                                          5)]
                                  int n,
                                  [Values(TestPayload.TaskType.Compute, TestPayload.TaskType.Transfer)]
                                  TestPayload.TaskType taskType)
  {
    var sessionId = Guid.NewGuid() + "mytestsession";


    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
      MaxRetries  = 2,
      Priority    = 1,
    };

    Console.WriteLine("Creating Session");
    var session = client_.CreateSession(new CreateSessionRequest
    {
      DefaultTaskOption = taskOptions,
      Id                = sessionId,
    });
    switch (session.ResultCase)
    {
      case CreateSessionReply.ResultOneofCase.Error:
        throw new Exception("Error while creating session : " + session.Error);
      case CreateSessionReply.ResultOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateSessionReply.ResultOneofCase.Ok:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    Console.WriteLine("Session Created");

    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var taskId = "multi" + i + "-" + Guid.NewGuid();

      var payload = new TestPayload
      {
        Type      = taskType,
        DataBytes = BitConverter.GetBytes(i),
        ResultKey = taskId,
      };

      var req = new TaskRequest
      {
        Id      = taskId,
        Payload = ByteString.CopyFrom(payload.Serialize()),
        ExpectedOutputKeys =
        {
          taskId,
        },
      };
      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    var createTaskReply = await client_.CreateTasksAsync(sessionId,
                                                         taskOptions,
                                                         taskRequestList);

    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        Console.WriteLine("Task Created");
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    var waitForCompletion = client_.WaitForCompletion(new WaitRequest
    {
      Filter = new TaskFilter
      {
        Task = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            taskRequestList.Select(request => request.Id),
          },
        },
      },
      StopOnFirstTaskCancellation = true,
      StopOnFirstTaskError        = true,
    });

    Console.WriteLine(waitForCompletion.ToString());

    var resultAvailability = taskRequestList.Select(request =>
    {
      var resultRequest = new ResultRequest
      {
        Key     = request.Id,
        Session = sessionId,
      };
      var availabilityReply = client_.WaitForAvailability(resultRequest);
      Console.WriteLine(availabilityReply.ToString());
      return availabilityReply.TypeCase;
    });

    Assert.IsTrue(resultAvailability.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultTypeOneofCases = taskRequestList.Select(request =>
    {
      var resultRequest = new ResultRequest
      {
        Key     = request.Id,
        Session = sessionId,
      };
      var taskOutput = client_.TryGetTaskOutput(resultRequest);
      Console.WriteLine(taskOutput.ToString());
      return taskOutput.TypeCase;
    });

    Assert.IsTrue(resultTypeOneofCases.All(t => t == Output.TypeOneofCase.Ok));

    var resultList = taskRequestList.Select(async request =>
    {
      var resultRequest = new ResultRequest
      {
        Key     = request.Id,
        Session = sessionId,
      };

      var resultPayload = TestPayload.Deserialize(await client_.GetResultAsync(resultRequest));
      Console.WriteLine($"Payload Type : {resultPayload.Type}");
      if (resultPayload.Type == TestPayload.TaskType.Result)
      {
        var output = BitConverter.ToInt32(resultPayload.DataBytes);
        Console.WriteLine($"Result : {output}");
        return output;
      }

      return 0;
    });

    var sum = resultList.Aggregate((t1, t2) => Task.FromResult(t1.Result + t2.Result));
    Assert.AreEqual(n * (n - 1) * (2 * n - 1) / 6,
                    sum.Result);
  }

  [Test]
  public async Task MultipleDataDependencies([Values(1,
                                                     5, 20)]
                                             int n)
  {
    var sessionId = Guid.NewGuid() + "-MultipleDatadependencies";


    var taskOptions = new TaskOptions
    {
      MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
      MaxRetries = 2,
      Priority = 1,
    };

    Console.WriteLine("Creating Session");
    var session = client_.CreateSession(new CreateSessionRequest
    {
      DefaultTaskOption = taskOptions,
      Id = sessionId,
    });
    switch (session.ResultCase)
    {
      case CreateSessionReply.ResultOneofCase.Error:
        throw new Exception("Error while creating session : " + session.Error);
      case CreateSessionReply.ResultOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateSessionReply.ResultOneofCase.Ok:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    Console.WriteLine("Session Created");

    var taskRequestList = new List<TaskRequest>();

    for (var i = 0; i < n; i++)
    {
      var taskId = "datadep-" + i + "-" + Guid.NewGuid();

      var payload = new TestPayload
      {
        Type      = TestPayload.TaskType.DatadepTransfer,
        DataBytes = BitConverter.GetBytes(i + 5),
        ResultKey = taskId,
      };

      var req = new TaskRequest
      {
        Id = taskId,
        Payload = ByteString.CopyFrom(payload.Serialize()),
        ExpectedOutputKeys =
        {
          taskId + "-res1",
          taskId + "-res2",
        },
      };
      taskRequestList.Add(req);
    }

    Console.WriteLine("TaskRequest Created");

    var createTaskReply = await client_.CreateTasksAsync(sessionId,
                                                         taskOptions,
                                                         taskRequestList);

    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        Console.WriteLine("Task Created");
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }

    var waitForCompletion = client_.WaitForCompletion(new WaitRequest
    {
      Filter = new TaskFilter
      {
        Task = new TaskFilter.Types.IdsRequest
        {
          Ids =
          {
            taskRequestList.Select(request => request.Id),
          },
        },
      },
      StopOnFirstTaskCancellation = true,
      StopOnFirstTaskError = true,
    });

    Console.WriteLine(waitForCompletion.ToString());

    var resultAvailability1 = taskRequestList.Select(request =>
    {
      var resultRequest = new ResultRequest
      {
        Key = request.Id + "-res1",
        Session = sessionId,
      };
      var availabilityReply = client_.WaitForAvailability(resultRequest);
      Console.WriteLine(availabilityReply.ToString());
      return availabilityReply.TypeCase;
    });

    Assert.IsTrue(resultAvailability1.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultAvailability2 = taskRequestList.Select(request =>
    {
      var resultRequest = new ResultRequest
      {
        Key     = request.Id + "-res1",
        Session = sessionId,
      };
      var availabilityReply = client_.WaitForAvailability(resultRequest);
      Console.WriteLine(availabilityReply.ToString());
      return availabilityReply.TypeCase;
    });

    Assert.IsTrue(resultAvailability2.All(t => t == AvailabilityReply.TypeOneofCase.Ok));

    var resultTypeOneofCases = taskRequestList.Select(request =>
    {
      var resultRequest = new ResultRequest
      {
        Key = request.Id,
        Session = sessionId,
      };
      var taskOutput = client_.TryGetTaskOutput(resultRequest);
      Console.WriteLine(taskOutput.ToString());
      return taskOutput.TypeCase;
    });

    Assert.IsTrue(resultTypeOneofCases.All(t => t == Output.TypeOneofCase.Ok));

    var results = taskRequestList.Select(async request =>
    {
      var resultRequest1 = new ResultRequest
      {
        Key = request.Id + "-res1",
        Session = sessionId,
      };
      var resultBytes1   = await client_.GetResultAsync(resultRequest1);
      if (resultBytes1.Length == 0)
      {
        throw new Exception();
      }
      var resultPayload1 = TestPayload.Deserialize(resultBytes1);

      var resultRequest2 = new ResultRequest
      {
        Key     = request.Id + "-res2",
        Session = sessionId,
      };
      var resultBytes2 = await client_.GetResultAsync(resultRequest2);
      if (resultBytes2.Length == 0)
      {
        throw new Exception();
      }
      var resultPayload2 = TestPayload.Deserialize(resultBytes2);

      var resultInt1 = BitConverter.ToInt32(resultPayload1.DataBytes);
      var resultInt2 = BitConverter.ToInt32(resultPayload2.DataBytes);

      Console.WriteLine($"Result1 {resultInt1}");
      Console.WriteLine($"Result2 {resultInt2}");

      return 2 * resultInt2 == resultInt1;
    });
    Assert.IsTrue(results.All(task => task.Result));
  }
}