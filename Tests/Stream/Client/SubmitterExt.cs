using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Stream.Client;

using Google.Protobuf.WellKnownTypes;

namespace ArmoniK.Extensions.Common.StreamWrapper.Tests.Client;

public static class SubmitterExt
{
  public static void CreateSessionAndCheckReply(this Submitter.SubmitterClient client,
                                                string                         sessionId)
  {
    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromHours(1)),
                        MaxRetries  = 3,
                        Priority    = 1,
                      };
    Console.WriteLine("Creating Session");
    var session = client.CreateSession(new CreateSessionRequest
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
  }

  public static async Task CreateTasksAndCheckReplyAsync(this Submitter.SubmitterClient client,
                                                         string                         sessionId,
                                                         TaskOptions                    taskOptions,
                                                         IEnumerable<TaskRequest>       taskRequestList)
  {
    var createTaskReply = await client.CreateTasksAsync(sessionId,
                                                        taskOptions,
                                                        taskRequestList)
                                      .ConfigureAwait(false);
    switch (createTaskReply.DataCase)
    {
      case CreateTaskReply.DataOneofCase.NonSuccessfullIds:
        throw new Exception($"NonSuccessfullIds : {createTaskReply.NonSuccessfullIds}");
      case CreateTaskReply.DataOneofCase.None:
        throw new Exception("Issue with Server !");
      case CreateTaskReply.DataOneofCase.Successfull:
        break;
      default:
        throw new ArgumentOutOfRangeException();
    }
  }
}