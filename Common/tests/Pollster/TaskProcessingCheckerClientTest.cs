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

using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Pollster.TaskProcessingChecker;

using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Pollster;

[TestFixture(TestOf = typeof(TaskProcessingCheckerClient))]
public class TaskProcessingCheckerClientTest
{
  private sealed class StaticResponseHandler : HttpMessageHandler
  {
    private readonly string body_;

    public StaticResponseHandler(string body)
      => body_ = body;

    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
                                                           CancellationToken  cancellationToken)
      => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                         {
                           Content = new StringContent(body_),
                         });
  }

  private static Task<IReadOnlyCollection<string>> GetProcessingMessageIds(string body,
                                                                          string taskId)
  {
    var factory = new Mock<IHttpClientFactory>();
    factory.Setup(f => f.CreateClient(It.IsAny<string>()))
           .Returns(() => new HttpClient(new StaticResponseHandler(body)));

    return new TaskProcessingCheckerClient(factory.Object,
                                           NullLogger<TaskProcessingCheckerClient>.Instance).GetProcessingMessageIds(taskId,
                                                                                                                     "pod",
                                                                                                                     CancellationToken.None);
  }

  [TestCase("{\"task\":[\"message\"]}",
            ExpectedResult = new[]
                             {
                               "message",
                             })]
  [TestCase("{\"task\":[\"message1\",\"message2\"],\"other\":[\"message3\"]}",
            ExpectedResult = new[]
                             {
                               "message1",
                               "message2",
                             })]
  [TestCase("{\"other\":[\"message\"]}",
            ExpectedResult = new string[0])]
  [TestCase("{}",
            ExpectedResult = new string[0])]
  [TestCase("null",
            ExpectedResult = new string[0])]
  [TestCase("task,other",
            ExpectedResult = new string[0])]
  [TestCase("",
            ExpectedResult = new string[0])]
  public Task<IReadOnlyCollection<string>> GetProcessingMessageIdsShouldReturnMessagesOfTask(string body)
    => GetProcessingMessageIds(body,
                               "task");
}
