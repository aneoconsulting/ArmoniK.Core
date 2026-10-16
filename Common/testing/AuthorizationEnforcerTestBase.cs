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
using System.Linq;
using System.Linq.Expressions;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Authorization;
using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Meter;
using ArmoniK.Core.Common.Pollster;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;
using ArmoniK.Core.Utils.Uuid;

using Grpc.AspNetCore.Server;
using Grpc.Core;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

using CreateSessionRequest = ArmoniK.Api.gRPC.V1.Sessions.CreateSessionRequest;
using FilterField = ArmoniK.Api.gRPC.V1.Results.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Results.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Results.FiltersAnd;
using Output = ArmoniK.Core.Common.Storage.Output;
using Result = ArmoniK.Core.Common.Storage.Result;
using ResultStatus = ArmoniK.Core.Common.Storage.ResultStatus;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;
using TaskStatus = ArmoniK.Core.Common.Storage.TaskStatus;

namespace ArmoniK.Core.Common.Tests.TestBase;

/// <summary>
///   Requests decided by the enforcer over sessions, tasks and results stored by each adaptor
/// </summary>
[TestFixture]
public class AuthorizationEnforcerTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    GetTablesInstance();

    if (!RunTests)
    {
      return;
    }

    await SessionTable!.Init(CancellationToken.None)
                       .ConfigureAwait(false);
    await TaskTable!.Init(CancellationToken.None)
                    .ConfigureAwait(false);
    await ResultTable!.Init(CancellationToken.None)
                      .ConfigureAwait(false);

    aliceSession_ = await SessionTable.SetSessionDataAsync(["part0"],
                                                           Options,
                                                           "cn:alice",
                                                           ["role:modelling-team"])
                                      .ConfigureAwait(false);
    otherSession_ = await SessionTable.SetSessionDataAsync(["part0"],
                                                           Options,
                                                           "cn:eve",
                                                           ["role:other-team"])
                                      .ConfigureAwait(false);

    await ResultTable.Create([
                       Result(aliceSession_,
                              "r-alice"),
                       Result(otherSession_,
                              "r-other"),
                     ])
                     .ConfigureAwait(false);
    await TaskTable.CreateTasks([
                     Task(aliceSession_,
                          "t-alice"),
                     Task(otherSession_,
                          "t-other"),
                   ])
                   .ConfigureAwait(false);

    var sets = AuthorizationConfiguration.PermissionSets(ExamplePermissionSets.With(new PermissionSetOption
                                                                                    {
                                                                                      Name        = "team-reader",
                                                                                      Permissions = ["data:read", "data:download"],
                                                                                    },
                                                                                    new PermissionSetOption
                                                                                    {
                                                                                      Name        = "team-lister",
                                                                                      Permissions = ["data:read"],
                                                                                    }));
    engine_ = DecisionEngine.Build([
                                     new GrantDefinition("team-create",
                                                         "role:modelling-team",
                                                         "session-create",
                                                         "request.partition == \"part0\""),
                                     new GrantDefinition("team-own",
                                                         "role:modelling-team",
                                                         "session-own",
                                                         "session.owner == request.caller && grant.principal in session.createdUnder"),
                                     new GrantDefinition("lead-supervise",
                                                         "role:lead",
                                                         "session-supervise",
                                                         "\"role:modelling-team\" in session.createdUnder"),
                                     new GrantDefinition("lead-read",
                                                         "role:lead",
                                                         "team-reader",
                                                         "\"role:modelling-team\" in session.createdUnder"),
                                     new GrantDefinition("support",
                                                         "role:support",
                                                         "session-supervise",
                                                         "\"role:modelling-team\" in session.createdUnder"),
                                     new GrantDefinition("platform",
                                                         "role:platform-team",
                                                         "cluster-admin",
                                                         null),
                                     new GrantDefinition("audit-team",
                                                         "role:auditor",
                                                         "team-lister",
                                                         "\"role:modelling-team\" in session.createdUnder"),
                                     new GrantDefinition("audit-other",
                                                         "role:auditor",
                                                         "team-reader",
                                                         "\"role:other-team\" in session.createdUnder"),
                                   ],
                                   sets,
                                   out var rejected);
    Assert.That(rejected,
                Is.Empty);

    audit_    = new RecordingLoggerFactory();
    enforcer_ = Enforcer(SessionTable);
  }

  [TearDown]
  public virtual void TearDown()
  {
    SessionTable = null;
    TaskTable    = null;
    ResultTable  = null;
    RunTests     = false;
  }

  private AuthorizationEnforcer Enforcer(ISessionTable sessionTable)
    => new(new FixedEngine(engine_!),
           new CallerResolver(new IdentityOptions()),
           new AuthorizationAudit(audit_!),
           sessionTable,
           TaskTable!,
           ResultTable!,
           new Injection.Options.Submitter
           {
             DefaultPartition = "part0",
           },
           NullLogger<AuthorizationEnforcer>.Instance);

  protected ISessionTable? SessionTable;
  protected ITaskTable?    TaskTable;
  protected IResultTable?  ResultTable;
  protected bool           RunTests;

  public virtual void GetTablesInstance()
  {
  }

  private static readonly TaskOptions Options = new(new Dictionary<string, string>(),
                                                    TimeSpan.FromMinutes(1),
                                                    1,
                                                    1,
                                                    "part0",
                                                    "",
                                                    "",
                                                    "",
                                                    "",
                                                    "");

  private string                  aliceSession_ = "";
  private RecordingLoggerFactory? audit_;
  private DecisionEngine?         engine_;
  private AuthorizationEnforcer?  enforcer_;
  private string                  otherSession_ = "";

  private static Result Result(string session,
                               string id)
    => new(session,
           id,
           id,
           "",
           "",
           "",
           ResultStatus.Created,
           [],
           DateTime.UtcNow,
           null,
           0,
           Encoding.UTF8.GetBytes(id),
           false);

  private static TaskData Task(string session,
                               string id)
    => new(session,
           id,
           "",
           "",
           "payload",
           "",
           [],
           [],
           [],
           [],
           TaskStatus.Submitted,
           Options,
           new Output(OutputStatus.Error,
                      ""));

  private static string Certificate(string subject)
  {
    using var key = RSA.Create(2048);
    using var certificate = new CertificateRequest(subject,
                                                   key,
                                                   HashAlgorithmName.SHA256,
                                                   RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                                                                               DateTimeOffset.UtcNow.AddDays(1));
    return Uri.EscapeDataString(certificate.ExportCertificatePem());
  }

  private const string Alice    = "CN=alice, OU=modelling-team";
  private const string Lead     = "CN=lead, OU=lead";
  private const string Support  = "CN=support, OU=support";
  private const string Stranger = "CN=stranger";
  private const string Admin    = "CN=admin, OU=platform-team";
  private const string Auditor  = "CN=auditor, OU=auditor";

  private static TestServerCallContext Context(string? subject)
  {
    var context = TestServerCallContext.Create();
    var http    = new DefaultHttpContext();
    if (subject is not null)
    {
      http.Request.Headers["X-Forwarded-Client-Cert"] = Certificate(subject);
    }

    context.UserState["__HttpContext"] = http;
    return context;
  }

  // As the authentication middleware does, before the request is read and decided
  private static async Task Authenticate(AuthorizationEnforcer enforcer,
                                         string                service,
                                         string                method,
                                         ServerCallContext     context)
  {
    var http = context.GetHttpContext();
    http.Items[AuthorizationEnforcer.CallerKey] = await enforcer.AuthenticateAsync(service,
                                                                                   method,
                                                                                   MethodAuthorization.Rules[(service, method)],
                                                                                   http)
                                                                .ConfigureAwait(false);
  }

  private async Task<ServerCallContext> Authorize(string? subject,
                                                  string  service,
                                                  string  method,
                                                  object  request)
  {
    var context = Context(subject);
    await Authenticate(enforcer_!,
                       service,
                       method,
                       context)
      .ConfigureAwait(false);
    await enforcer_!.AuthorizeAsync(service,
                                    method,
                                    MethodAuthorization.Rules[(service, method)],
                                    request,
                                    context)
                    .ConfigureAwait(false);
    return context;
  }

  private Task<ServerCallContext> GetSession(string? subject,
                                             string  session)
    => Authorize(subject,
                 nameof(GrpcSessionsService),
                 nameof(GrpcSessionsService.GetSession),
                 new GetSessionRequest
                 {
                   SessionId = session,
                 });

  [Test]
  public async Task TheOwnerAndTheLeadSeeTheTeamSession()
  {
    if (RunTests)
    {
      await GetSession(Alice,
                       aliceSession_)
        .ConfigureAwait(false);
      await GetSession(Lead,
                       aliceSession_)
        .ConfigureAwait(false);
      Assert.That(audit_!.Messages.Count(message => message.Contains("Allow")),
                  Is.EqualTo(2));
    }
  }

  [Test]
  public async Task ACallerAuthenticatedBeforeTheRequestIsNotAuthenticatedAgain()
  {
    if (RunTests)
    {
      var authenticated = Context(Alice);
      var caller = await new CallerResolver(new IdentityOptions()).ResolveAsync(authenticated.GetHttpContext())
                                                                  .ConfigureAwait(false);

      // No credential left in the headers: only the caller kept by the authentication middleware can be used
      var context = Context(null);
      context.GetHttpContext()
             .Items[AuthorizationEnforcer.CallerKey] = caller;
      await enforcer_!.AuthorizeAsync(nameof(GrpcSessionsService),
                                      nameof(GrpcSessionsService.GetSession),
                                      MethodAuthorization.Rules[(nameof(GrpcSessionsService), nameof(GrpcSessionsService.GetSession))],
                                      new GetSessionRequest
                                      {
                                        SessionId = aliceSession_,
                                      },
                                      context)
                      .ConfigureAwait(false);

      Assert.That(audit_!.Messages.Count(message => message.Contains("Allow")),
                  Is.EqualTo(1));
    }
  }

  private async Task<(bool Alice, bool Other)> DataReferenceShown(string subject)
  {
    var context = await Authorize(subject,
                                  nameof(GrpcResultsService),
                                  nameof(GrpcResultsService.ListResults),
                                  new ListResultsRequest())
                    .ConfigureAwait(false);
    var shown = await context.DataReferenceShownAsync(SessionTable!,
                                                      [aliceSession_, otherSession_])
                             .ConfigureAwait(false);
    return (shown(aliceSession_), shown(otherSession_));
  }

  [Test]
  public async Task TheDataReferenceIsShownOnlyWhereTheCallerMayDownload()
  {
    if (RunTests)
    {
      // data:read only on the team sessions, data:download too on the other team sessions
      Assert.That(await DataReferenceShown(Auditor)
                    .ConfigureAwait(false),
                  Is.EqualTo((false, true)));
      Assert.That(await DataReferenceShown(Lead)
                    .ConfigureAwait(false),
                  Is.EqualTo((true, false)));
      Assert.That(await DataReferenceShown(Admin)
                    .ConfigureAwait(false),
                  Is.EqualTo((true, true)));
    }
  }

  [Test]
  public async Task AListingOfResultsShowsTheDataReferenceOnlyWhereTheCallerMayDownload()
  {
    if (RunTests)
    {
      var services = new ServiceCollection().AddMetrics()
                                            .AddSingleton<AgentIdentifier>()
                                            .AddSingleton<MeterHolder>()
                                            .AddSingleton(typeof(FunctionExecutionMetrics<>))
                                            .BuildServiceProvider();
      var service = new GrpcResultsService(ResultTable!,
                                           TaskTable!,
                                           SessionTable!,
                                           Mock.Of<IObjectStorage>(),
                                           Mock.Of<IPushQueueStorage>(),
                                           services.GetRequiredService<FunctionExecutionMetrics<GrpcResultsService>>(),
                                           new Injection.Options.Submitter(),
                                           Mock.Of<IUuidGenerator>(),
                                           NullLogger<GrpcResultsService>.Instance);
      var request = new ListResultsRequest
                    {
                      Filters  = new Filters(),
                      PageSize = 10,
                    };

      var response = await service.ListResults(request,
                                               await Authorize(Auditor,
                                                               nameof(GrpcResultsService),
                                                               nameof(GrpcResultsService.ListResults),
                                                               request)
                                                 .ConfigureAwait(false))
                                  .ConfigureAwait(false);

      Assert.That(response.Results.ToDictionary(result => result.ResultId,
                                                result => result.OpaqueId.ToStringUtf8()),
                  Is.EquivalentTo(new Dictionary<string, string>
                                  {
                                    ["r-alice"] = "",
                                    ["r-other"] = "r-other",
                                  }));
    }
  }

  [Test]
  public async Task WithoutAuthorizationTheDataReferenceIsShown()
  {
    if (RunTests)
    {
      var shown = await TestServerCallContext.Create()
                                             .DataReferenceShownAsync(SessionTable!,
                                                                      [aliceSession_])
                                             .ConfigureAwait(false);
      Assert.That(shown(aliceSession_),
                  Is.True);
    }
  }

  [Test]
  public async Task ASessionTheCallerCannotSeeIsAnsweredLikeAMissingOne()
  {
    if (RunTests)
    {
      var invisible = Assert.ThrowsAsync<SessionNotFoundException>(() => GetSession(Alice,
                                                                                    otherSession_));
      var missing = Assert.ThrowsAsync<SessionNotFoundException>(() => GetSession(Alice,
                                                                                  "missing"));
      Assert.That(invisible!.Message,
                  Is.EqualTo(missing!.Message));
      Assert.ThrowsAsync<SessionNotFoundException>(() => GetSession(Stranger,
                                                                    aliceSession_));
      await GetSession(Admin,
                       otherSession_)
        .ConfigureAwait(false);
    }
  }

  [Test]
  public async Task AFaultIsAnsweredAsUnavailable()
  {
    if (RunTests)
    {
      var enforcer = new AuthorizationEnforcer(new FailingEngine(),
                                               new CallerResolver(new IdentityOptions()),
                                               new AuthorizationAudit(audit_!),
                                               SessionTable!,
                                               TaskTable!,
                                               ResultTable!,
                                               new Injection.Options.Submitter
                                               {
                                                 DefaultPartition = "part0",
                                               },
                                               NullLogger<AuthorizationEnforcer>.Instance);

      var context = Context(Alice);
      await Authenticate(enforcer,
                         nameof(GrpcSessionsService),
                         nameof(GrpcSessionsService.GetSession),
                         context)
        .ConfigureAwait(false);
      Assert.ThrowsAsync<AuthorizationUnavailableException>(() => enforcer.AuthorizeAsync(nameof(GrpcSessionsService),
                                                                                          nameof(GrpcSessionsService.GetSession),
                                                                                          MethodAuthorization.Rules[(nameof(GrpcSessionsService),
                                                                                                                     nameof(GrpcSessionsService.GetSession))],
                                                                                          new GetSessionRequest
                                                                                          {
                                                                                            SessionId = aliceSession_,
                                                                                          },
                                                                                          context));
      Assert.That(audit_!.Messages.Any(message => message.Contains("Fault")),
                  Is.True);
    }
  }

  [Test]
  public void ACredentialThatCannotBeCheckedIsAnsweredAsUnavailable()
  {
    if (RunTests)
    {
      var enforcer = new AuthorizationEnforcer(new FailingEngine(),
                                               new CallerResolver(new IdentityOptions
                                                                  {
                                                                    ValidateTokens = false,
                                                                  }),
                                               new AuthorizationAudit(audit_!),
                                               SessionTable!,
                                               TaskTable!,
                                               ResultTable!,
                                               new Injection.Options.Submitter
                                               {
                                                 DefaultPartition = "part0",
                                               },
                                               NullLogger<AuthorizationEnforcer>.Instance);
      var context = TestServerCallContext.Create();
      var http = new DefaultHttpContext
                 {
                   RequestServices = new ServiceCollection().AddSingleton<IAuthenticationService>(new UnreachableAuthority())
                                                            .BuildServiceProvider(),
                 };
      http.Request.Headers.Authorization = "Bearer token";
      context.UserState["__HttpContext"] = http;

      var exception = Assert.ThrowsAsync<RpcException>(() => Authenticate(enforcer,
                                                                          nameof(GrpcSessionsService),
                                                                          nameof(GrpcSessionsService.GetSession),
                                                                          context));
      Assert.That(exception!.StatusCode,
                  Is.EqualTo(StatusCode.Unavailable));
      Assert.That(audit_!.Messages,
                  Has.One.Matches<string>(message => message.Contains("Fault") && message.Contains(UnreachableAuthority.Message)));
    }
  }

  [TestCase(nameof(GrpcSubmitterService.CreateLargeTasks),
            "Tasks.SubmitTasks")]
  [TestCase(nameof(GrpcSubmitterService.CancelTasks),
            "Tasks.CancelTasks")]
  [TestCase(nameof(GrpcSubmitterService.CountTasks),
            "Tasks.CountTasksByStatus")]
  [TestCase(nameof(GrpcSubmitterService.ListTasks),
            "Tasks.ListTasks")]
  [TestCase(nameof(GrpcSubmitterService.WaitForCompletion),
            "Events.GetEvents")]
  [TestCase(nameof(GrpcSubmitterService.ListSessions),
            "Sessions.ListSessions")]
  public void AnUnsupportedLegacyMethodNamesWhatToUseInstead(string method,
                                                             string instead)
  {
    if (RunTests)
    {
      var exception = Assert.ThrowsAsync<RpcException>(() => Authorize(Alice,
                                                                       nameof(GrpcSubmitterService),
                                                                       method,
                                                                       new object()));
      Assert.That(exception!.StatusCode,
                  Is.EqualTo(StatusCode.PermissionDenied));
      Assert.That(exception.Status.Detail,
                  Does.Contain(instead));
    }
  }

  [Test]
  public async Task WhatTheDecisionReadIsLeftForTheHandler()
  {
    if (RunTests)
    {
      var resultContext = await Authorize(Alice,
                                          nameof(GrpcResultsService),
                                          nameof(GrpcResultsService.GetResult),
                                          new GetResultRequest
                                          {
                                            ResultId = "r-alice",
                                          })
                            .ConfigureAwait(false);
      var sessionContext = await GetSession(Alice,
                                            aliceSession_)
                             .ConfigureAwait(false);

      var stored = await ResultTable!.GetResult("r-alice")
                                     .ConfigureAwait(false);

      Assert.That(resultContext.ResolvedResult("r-alice"),
                  Is.EqualTo(stored)
                    .Using<Result>((a,
                                    b) => a.ResultId == b.ResultId && a.SessionId == b.SessionId && a.Name == b.Name && a.Status == b.Status &&
                                          a.OpaqueId.SequenceEqual(b.OpaqueId)));
      Assert.That(resultContext.ResolvedSession(aliceSession_)
                               ?.Owner,
                  Is.EqualTo("cn:alice"));
      Assert.That(sessionContext.ResolvedSession(aliceSession_)
                                ?.CreatedUnder,
                  Is.EqualTo(new[]
                             {
                               "role:modelling-team",
                             }));
      Assert.That(sessionContext.ResolvedResult("r-alice"),
                  Is.Null);
    }
  }

  [Test]
  public async Task OnlyTheHandlersReadingAResultAreLeftItsRow()
  {
    if (RunTests)
    {
      var context = await Authorize(Alice,
                                    nameof(GrpcResultsService),
                                    nameof(GrpcResultsService.DeleteResultsData),
                                    new DeleteResultsDataRequest
                                    {
                                      SessionId = aliceSession_,
                                      ResultId =
                                      {
                                        "r-alice",
                                      },
                                    })
                      .ConfigureAwait(false);

      Assert.That(context.ResolvedResult("r-alice"),
                  Is.Null);
    }
  }

  [Test]
  public async Task TheOwnerTasksAreAnsweredFromTheResultsTheDecisionRead()
  {
    if (RunTests)
    {
      var request = new GetOwnerTaskIdRequest
                    {
                      SessionId = aliceSession_,
                      ResultId =
                      {
                        "r-alice",
                      },
                    };
      var context = await Authorize(Alice,
                                    nameof(GrpcResultsService),
                                    nameof(GrpcResultsService.GetOwnerTaskId),
                                    request)
                      .ConfigureAwait(false);

      // Any read of the result table fails the call
      var response = await ResultsService(Mock.Of<IResultTable>(MockBehavior.Strict))
                           .GetOwnerTaskId(request,
                                           context)
                           .ConfigureAwait(false);

      Assert.That(response.ResultTask.Select(map => map.ResultId),
                  Is.EqualTo(new[]
                             {
                               "r-alice",
                             }));
    }
  }

  [Test]
  public async Task WithoutAuthorizationTheOwnerTasksAreRead()
  {
    if (RunTests)
    {
      var response = await ResultsService(ResultTable!)
                           .GetOwnerTaskId(new GetOwnerTaskIdRequest
                                           {
                                             SessionId = aliceSession_,
                                             ResultId =
                                             {
                                               "r-alice",
                                             },
                                           },
                                           TestServerCallContext.Create())
                           .ConfigureAwait(false);

      Assert.That(response.ResultTask.Select(map => map.ResultId),
                  Is.EqualTo(new[]
                             {
                               "r-alice",
                             }));
    }
  }

  private GrpcResultsService ResultsService(IResultTable resultTable)
  {
    var services = new ServiceCollection().AddMetrics()
                                          .AddSingleton<AgentIdentifier>()
                                          .AddSingleton<MeterHolder>()
                                          .AddSingleton(typeof(FunctionExecutionMetrics<>))
                                          .BuildServiceProvider();
    return new GrpcResultsService(resultTable,
                                  TaskTable!,
                                  SessionTable!,
                                  Mock.Of<IObjectStorage>(),
                                  Mock.Of<IPushQueueStorage>(),
                                  services.GetRequiredService<FunctionExecutionMetrics<GrpcResultsService>>(),
                                  new Injection.Options.Submitter(),
                                  Mock.Of<IUuidGenerator>(),
                                  NullLogger<GrpcResultsService>.Instance);
  }

  [Test]
  public async Task SubmittingCannotUseAResultOfAnotherSession()
  {
    if (RunTests)
    {
      SubmitTasksRequest Submit(string payload)
        => new()
           {
             SessionId = aliceSession_,
             TaskCreations =
             {
               new SubmitTasksRequest.Types.TaskCreation
               {
                 PayloadId = payload,
               },
             },
           };

      await Authorize(Alice,
                      nameof(GrpcTasksService),
                      nameof(GrpcTasksService.SubmitTasks),
                      Submit("r-alice"))
        .ConfigureAwait(false);
      Assert.ThrowsAsync<ResultNotFoundException>(() => Authorize(Alice,
                                                                  nameof(GrpcTasksService),
                                                                  nameof(GrpcTasksService.SubmitTasks),
                                                                  Submit("r-other")));
    }
  }

  [Test]
  public void AnInvisibleTargetIsAnsweredLikeAMissingOneWhateverTheRequest()
  {
    if (RunTests)
    {
      Task<ServerCallContext> Download(string session)
        => Authorize(Alice,
                     nameof(GrpcResultsService),
                     nameof(GrpcResultsService.DownloadResultData),
                     new DownloadResultDataRequest
                     {
                       SessionId = session,
                       ResultId  = "",
                     });

      var invisibleSession = Assert.ThrowsAsync<ResultNotFoundException>(() => Download(otherSession_));
      var missingSession   = Assert.ThrowsAsync<ResultNotFoundException>(() => Download("missing"));
      Assert.That(invisibleSession!.Message,
                  Is.EqualTo(missingSession!.Message));

      Task<ServerCallContext> Output(string task)
        => Authorize(Alice,
                     nameof(GrpcSubmitterService),
                     nameof(GrpcSubmitterService.TryGetTaskOutput),
                     new TaskOutputRequest
                     {
                       Session = aliceSession_,
                       TaskId  = task,
                     });

      var otherTask   = Assert.ThrowsAsync<TaskNotFoundException>(() => Output("t-other"));
      var missingTask = Assert.ThrowsAsync<TaskNotFoundException>(() => Output("missing"));
      Assert.That(otherTask!.Message,
                  Is.EqualTo(missingTask!.Message));
    }
  }

  [Test]
  public async Task TheNamedSessionIsReadWhileTheTasksAreLookedUp()
  {
    if (RunTests)
    {
      var sessionTable = new Mock<ISessionTable>();
      sessionTable.Setup(table => table.FindSessionsAsync(It.IsAny<Expression<Func<SessionData, bool>>>(),
                                                          It.IsAny<Expression<Func<SessionData, SessionData>>>(),
                                                          It.IsAny<CancellationToken>()))
                  .Returns(AsyncEnumerable.Empty<SessionData>());
      var enforcer = Enforcer(sessionTable.Object);
      var context  = Context(Alice);
      await Authenticate(enforcer,
                         nameof(GrpcSubmitterService),
                         nameof(GrpcSubmitterService.TryGetTaskOutput),
                         context)
        .ConfigureAwait(false);

      // The task lookup refuses the request: the session was read already
      Assert.ThrowsAsync<TaskNotFoundException>(() => enforcer.AuthorizeAsync(nameof(GrpcSubmitterService),
                                                                              nameof(GrpcSubmitterService.TryGetTaskOutput),
                                                                              MethodAuthorization.Rules[(nameof(GrpcSubmitterService),
                                                                                                         nameof(GrpcSubmitterService.TryGetTaskOutput))],
                                                                              new TaskOutputRequest
                                                                              {
                                                                                Session = aliceSession_,
                                                                                TaskId  = "missing",
                                                                              },
                                                                              context));
      sessionTable.Verify(table => table.FindSessionsAsync(It.IsAny<Expression<Func<SessionData, bool>>>(),
                                                           It.IsAny<Expression<Func<SessionData, SessionData>>>(),
                                                           It.IsAny<CancellationToken>()),
                          Times.Once);
    }
  }

  [Test]
  public async Task WaitingForAResultNeedsToReadTheTaskProducingIt()
  {
    if (RunTests)
    {
      Task<ServerCallContext> Wait(string subject)
        => Authorize(subject,
                     nameof(GrpcSubmitterService),
                     nameof(GrpcSubmitterService.WaitForAvailability),
                     new ResultRequest
                     {
                       Session  = aliceSession_,
                       ResultId = "r-alice",
                     });

      // The answer carries the error of the task producing an aborted result: data:read alone is not enough
      Assert.That(Assert.ThrowsAsync<RpcException>(() => Wait(Auditor))!.StatusCode,
                  Is.EqualTo(StatusCode.PermissionDenied));
      await Wait(Lead)
        .ConfigureAwait(false);
    }
  }

  [Test]
  public async Task AResultIsDecidedOnItsOwnSession()
  {
    if (RunTests)
    {
      await Authorize(Alice,
                      nameof(GrpcResultsService),
                      nameof(GrpcResultsService.DownloadResultData),
                      new DownloadResultDataRequest
                      {
                        SessionId = aliceSession_,
                        ResultId  = "r-alice",
                      })
        .ConfigureAwait(false);
      Assert.ThrowsAsync<ResultNotFoundException>(() => Authorize(Alice,
                                                                  nameof(GrpcResultsService),
                                                                  nameof(GrpcResultsService.DownloadResultData),
                                                                  new DownloadResultDataRequest
                                                                  {
                                                                    SessionId = aliceSession_,
                                                                    ResultId  = "r-other",
                                                                  }));
      // Even for a caller who sees both sessions, the result must belong to the session named
      Assert.ThrowsAsync<ResultNotFoundException>(() => Authorize(Admin,
                                                                  nameof(GrpcResultsService),
                                                                  nameof(GrpcResultsService.DownloadResultData),
                                                                  new DownloadResultDataRequest
                                                                  {
                                                                    SessionId = aliceSession_,
                                                                    ResultId  = "r-other",
                                                                  }));
      Assert.ThrowsAsync<ResultNotFoundException>(() => Authorize(Admin,
                                                                  nameof(GrpcTasksService),
                                                                  nameof(GrpcTasksService.SubmitTasks),
                                                                  new SubmitTasksRequest
                                                                  {
                                                                    SessionId = aliceSession_,
                                                                    TaskCreations =
                                                                    {
                                                                      new SubmitTasksRequest.Types.TaskCreation
                                                                      {
                                                                        PayloadId = "r-other",
                                                                      },
                                                                    },
                                                                  }));
      Assert.ThrowsAsync<ResultNotFoundException>(() => Authorize(Alice,
                                                                  nameof(GrpcResultsService),
                                                                  nameof(GrpcResultsService.GetResult),
                                                                  new GetResultRequest
                                                                  {
                                                                    ResultId = "r-other",
                                                                  }));
    }
  }

  [Test]
  public async Task ARequestNamingNothingIsAllowed()
  {
    if (RunTests)
    {
      await Authorize(Alice,
                      nameof(GrpcTasksService),
                      nameof(GrpcTasksService.CancelTasks),
                      new CancelTasksRequest())
        .ConfigureAwait(false);
      await Authorize(Stranger,
                      nameof(GrpcTasksService),
                      nameof(GrpcTasksService.GetResultIds),
                      new GetResultIdsRequest())
        .ConfigureAwait(false);

      Assert.That(audit_!.Messages.Count(message => message.Contains("Allow")),
                  Is.EqualTo(2));
    }
  }

  [Test]
  public void ARequestNamingOnlyEmptyIdsIsAnsweredLikeAMissingResult()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<ResultNotFoundException>(() => Authorize(Alice,
                                                                  nameof(GrpcResultsService),
                                                                  nameof(GrpcResultsService.DownloadResultData),
                                                                  new DownloadResultDataRequest
                                                                  {
                                                                    SessionId = "",
                                                                    ResultId  = "",
                                                                  }));
      Assert.That(audit_!.Messages.Any(message => message.Contains("Allow")),
                  Is.False);
    }
  }

  // A call of Results.UploadResultData by Alice, as routed
  private async Task<TestServerCallContext> UploadContext()
  {
    var context = Context(Alice);
    context.GetHttpContext()
           .SetEndpoint(new Endpoint(null,
                                     new EndpointMetadataCollection(new GrpcMethodMetadata(typeof(GrpcResultsService),
                                                                                           new Method<UploadResultDataRequest, UploadResultDataResponse>(MethodType
                                                                                                                                                           .ClientStreaming,
                                                                                                                                                         "Results",
                                                                                                                                                         nameof(
                                                                                                                                                           GrpcResultsService
                                                                                                                                                             .UploadResultData),
                                                                                                                                                         Marshallers
                                                                                                                                                           .Create(_ =>
                                                                                                                                                                   [
                                                                                                                                                                   ],
                                                                                                                                                                   _ => new
                                                                                                                                                                     UploadResultDataRequest()),
                                                                                                                                                         Marshallers
                                                                                                                                                           .Create(_ =>
                                                                                                                                                                   [
                                                                                                                                                                   ],
                                                                                                                                                                   _ => new
                                                                                                                                                                     UploadResultDataResponse())))),
                                     "UploadResultData"));
    await Authenticate(enforcer_!,
                       nameof(GrpcResultsService),
                       nameof(GrpcResultsService.UploadResultData),
                       context)
      .ConfigureAwait(false);
    return context;
  }

  [Test]
  public async Task AnEmptyStreamIsRefused()
  {
    if (RunTests)
    {
      var handled = false;
      var context = await UploadContext()
                      .ConfigureAwait(false);

      Assert.That(Assert.ThrowsAsync<RpcException>(() => new AuthorizationInterceptor(enforcer_!)
                                                     .ClientStreamingServerHandler(new TestHelperAsyncStreamReader<UploadResultDataRequest>([]),
                                                                                   context,
                                                                                   async (stream,
                                                                                          _) =>
                                                                                   {
                                                                                     await stream.MoveNext(CancellationToken.None)
                                                                                                 .ConfigureAwait(false);
                                                                                     handled = true;
                                                                                     return new UploadResultDataResponse();
                                                                                   }))!.StatusCode,
                  Is.EqualTo(StatusCode.InvalidArgument));
      Assert.That(handled,
                  Is.False);
    }
  }

  [Test]
  public async Task AStreamRefusedOnItsFirstMessageStaysRefused()
  {
    if (RunTests)
    {
      var context = await UploadContext()
                      .ConfigureAwait(false);
      var upload = new UploadResultDataRequest
                   {
                     Id = new UploadResultDataRequest.Types.ResultIdentifier
                          {
                            SessionId = otherSession_,
                            ResultId  = "r-other",
                          },
                   };

      // A handler reading on after the refusal of the first message gets the refusal again, never the next messages
      Assert.ThrowsAsync<ResultNotFoundException>(() => new AuthorizationInterceptor(enforcer_!)
                                                    .ClientStreamingServerHandler(new TestHelperAsyncStreamReader<UploadResultDataRequest>([upload, upload]),
                                                                                  context,
                                                                                  async (stream,
                                                                                         _) =>
                                                                                  {
                                                                                    try
                                                                                    {
                                                                                      await stream.MoveNext(CancellationToken.None)
                                                                                                  .ConfigureAwait(false);
                                                                                    }
                                                                                    catch (ResultNotFoundException)
                                                                                    {
                                                                                    }

                                                                                    await stream.MoveNext(CancellationToken.None)
                                                                                                .ConfigureAwait(false);
                                                                                    return new UploadResultDataResponse();
                                                                                  }));
    }
  }

  [Test]
  public async Task ABatchIsAllowedWhole()
  {
    if (RunTests)
    {
      CancelTasksRequest Cancel(params string[] tasks)
        => new()
           {
             TaskIds =
             {
               tasks,
             },
           };

      Assert.ThrowsAsync<TaskNotFoundException>(() => Authorize(Alice,
                                                                nameof(GrpcTasksService),
                                                                nameof(GrpcTasksService.CancelTasks),
                                                                Cancel("t-alice",
                                                                       "t-other")));
      await Authorize(Lead,
                      nameof(GrpcTasksService),
                      nameof(GrpcTasksService.CancelTasks),
                      Cancel("t-alice"))
        .ConfigureAwait(false);
      Assert.That(Assert.ThrowsAsync<RpcException>(() => Authorize(Lead,
                                                                   nameof(GrpcTasksService),
                                                                   nameof(GrpcTasksService.SubmitTasks),
                                                                   new SubmitTasksRequest
                                                                   {
                                                                     SessionId = aliceSession_,
                                                                   }))!.StatusCode,
                  Is.EqualTo(StatusCode.PermissionDenied));
    }
  }

  [Test]
  public async Task CreatingASessionRecordsWhoItWasCreatedUnder()
  {
    if (RunTests)
    {
      var context = await Authorize(Alice,
                                    nameof(GrpcSessionsService),
                                    nameof(GrpcSessionsService.CreateSession),
                                    new CreateSessionRequest
                                    {
                                      PartitionIds =
                                      {
                                        "part0",
                                      },
                                      DefaultTaskOption = new Api.gRPC.V1.TaskOptions(),
                                    })
                      .ConfigureAwait(false);

      Assert.That(context.GetSessionAuthorship(),
                  Is.EqualTo(new SessionAuthorship("cn:alice",
                                                   ["role:modelling-team"]))
                    .Using<SessionAuthorship>((a,
                                               b) => a.Owner == b.Owner && a.CreatedUnder.SequenceEqual(b.CreatedUnder)));
      Assert.That(Assert.ThrowsAsync<RpcException>(() => Authorize(Alice,
                                                                   nameof(GrpcSessionsService),
                                                                   nameof(GrpcSessionsService.CreateSession),
                                                                   new CreateSessionRequest
                                                                   {
                                                                     PartitionIds =
                                                                     {
                                                                       "part0",
                                                                       "gpu",
                                                                     },
                                                                     DefaultTaskOption = new Api.gRPC.V1.TaskOptions(),
                                                                   }))!.StatusCode,
                  Is.EqualTo(StatusCode.PermissionDenied));
      Assert.ThrowsAsync<PartitionNotFoundException>(() => Authorize(Stranger,
                                                                     nameof(GrpcSessionsService),
                                                                     nameof(GrpcSessionsService.CreateSession),
                                                                     new CreateSessionRequest
                                                                     {
                                                                       DefaultTaskOption = new Api.gRPC.V1.TaskOptions(),
                                                                     }));
    }
  }

  [Test]
  public async Task ASessionIsDecidedOnItsDeclaredPartitionsAndUsesNoOther()
  {
    if (RunTests)
    {
      var engine = DecisionEngine.Build([
                                          new GrantDefinition("gpu-create",
                                                              "role:gpu-team",
                                                              "session-create",
                                                              "request.partition == \"gpu\""),
                                          new GrantDefinition("everyone-create",
                                                              "*",
                                                              "session-create",
                                                              "request.partition == \"part0\""),
                                        ],
                                        AuthorizationConfiguration.PermissionSets(ExamplePermissionSets.Options),
                                        out var rejected);
      Assert.That(rejected,
                  Is.Empty);
      var enforcer = new AuthorizationEnforcer(new FixedEngine(engine),
                                               new CallerResolver(new IdentityOptions()),
                                               new AuthorizationAudit(audit_!),
                                               SessionTable!,
                                               TaskTable!,
                                               ResultTable!,
                                               new Injection.Options.Submitter
                                               {
                                                 DefaultPartition = "part0",
                                               },
                                               NullLogger<AuthorizationEnforcer>.Instance);

      async Task<ServerCallContext> Create(string optionsPartition)
      {
        var context = Context("CN=gpu, OU=gpu-team");
        await Authenticate(enforcer,
                           nameof(GrpcSessionsService),
                           nameof(GrpcSessionsService.CreateSession),
                           context)
          .ConfigureAwait(false);
        await enforcer.AuthorizeAsync(nameof(GrpcSessionsService),
                                      nameof(GrpcSessionsService.CreateSession),
                                      MethodAuthorization.Rules[(nameof(GrpcSessionsService), nameof(GrpcSessionsService.CreateSession))],
                                      new CreateSessionRequest
                                      {
                                        PartitionIds =
                                        {
                                          "gpu",
                                        },
                                        DefaultTaskOption = new Api.gRPC.V1.TaskOptions
                                                            {
                                                              PartitionId = optionsPartition,
                                                            },
                                      },
                                      context)
                      .ConfigureAwait(false);
        return context;
      }

      // Created under the grants of its declared partition only: the grant for everyone on the default partition is
      // not recorded, so that it does not open the session to every caller
      var created = await Create("gpu")
                      .ConfigureAwait(false);
      Assert.That(created.GetSessionAuthorship()
                         .CreatedUnder,
                  Is.EqualTo(new[]
                             {
                               "role:gpu-team",
                             }));

      // Default task options in a partition that is not declared, explicitly or by default, are refused
      Assert.That(Assert.ThrowsAsync<RpcException>(() => Create(""))!.StatusCode,
                  Is.EqualTo(StatusCode.InvalidArgument));
      Assert.That(Assert.ThrowsAsync<RpcException>(() => Create("part0"))!.StatusCode,
                  Is.EqualTo(StatusCode.InvalidArgument));
    }
  }

  [Test]
  public async Task ApplicationsAreListedOverTheTasksTheCallerMayRead()
  {
    if (RunTests)
    {
      async Task<List<string>> Listed(string subject)
      {
        var context = await Authorize(subject,
                                      nameof(GrpcApplicationsService),
                                      nameof(GrpcApplicationsService.ListApplications),
                                      new ListApplicationsRequest())
                        .ConfigureAwait(false);
        return await TaskTable!.FindTasksAsync(context.RestrictListing<TaskData>(_ => true),
                                               task => task.TaskId)
                               .ToListAsync()
                               .ConfigureAwait(false);
      }

      // Under a condition, then across the cluster
      Assert.That(await Listed(Lead)
                    .ConfigureAwait(false),
                  Is.EqualTo(new[]
                             {
                               "t-alice",
                             }));
      Assert.That(await Listed(Admin)
                    .ConfigureAwait(false),
                  Is.EquivalentTo(new[]
                                  {
                                    "t-alice",
                                    "t-other",
                                  }));
    }
  }

  [Test]
  public async Task ListingsReturnWhatTheCallerHoldsThePermissionOn()
  {
    if (RunTests)
    {
      async Task<List<string>> Listed(string subject)
      {
        var context = await Authorize(subject,
                                      nameof(GrpcResultsService),
                                      nameof(GrpcResultsService.ListResults),
                                      new ListResultsRequest())
                        .ConfigureAwait(false);
        return await ResultTable!.GetResults(context.RestrictListing<Result>(_ => true),
                                             result => result.ResultId)
                                 .ToListAsync()
                                 .ConfigureAwait(false);
      }

      Assert.That(await Listed(Alice)
                    .ConfigureAwait(false),
                  Is.EqualTo(new[]
                             {
                               "r-alice",
                             }));
      Assert.That(await Listed(Lead)
                    .ConfigureAwait(false),
                  Is.EqualTo(new[]
                             {
                               "r-alice",
                             }));
      Assert.That(await Listed(Support)
                    .ConfigureAwait(false),
                  Is.Empty);
      Assert.That(await Listed(Admin)
                    .ConfigureAwait(false),
                  Is.EquivalentTo(new[]
                                  {
                                    "r-alice",
                                    "r-other",
                                  }));
    }
  }

  [Test]
  public async Task AListingNamingASessionResolvesOnlyThatSession()
  {
    if (RunTests)
    {
      var secondSession = await SessionTable!.SetSessionDataAsync(["part0"],
                                                                  Options,
                                                                  "cn:alice",
                                                                  ["role:modelling-team"])
                                             .ConfigureAwait(false);
      await ResultTable!.Create([
                          Result(secondSession,
                                 "r-alice-2"),
                        ])
                        .ConfigureAwait(false);

      async Task<List<string>> Visible(string session)
      {
        var context = await Authorize(Alice,
                                      nameof(GrpcResultsService),
                                      nameof(GrpcResultsService.ListResults),
                                      new ListResultsRequest
                                      {
                                        Filters = new Filters
                                                  {
                                                    Or =
                                                    {
                                                      new FiltersAnd
                                                      {
                                                        And =
                                                        {
                                                          new FilterField
                                                          {
                                                            Field = new ResultField
                                                                    {
                                                                      ResultRawField = new ResultRawField
                                                                                       {
                                                                                         Field = ResultRawEnumField.SessionId,
                                                                                       },
                                                                    },
                                                            FilterString = new FilterString
                                                                           {
                                                                             Value    = session,
                                                                             Operator = FilterStringOperator.Equal,
                                                                           },
                                                          },
                                                        },
                                                      },
                                                    },
                                                  },
                                      })
                        .ConfigureAwait(false);
        // Without the filter of the request: what the decision left reaches only the session it names
        return await ResultTable!.GetResults(context.RestrictListing<Result>(_ => true),
                                             result => result.ResultId)
                                 .ToListAsync()
                                 .ConfigureAwait(false);
      }

      Assert.That(await Visible(aliceSession_)
                    .ConfigureAwait(false),
                  Is.EqualTo(new[]
                             {
                               "r-alice",
                             }));
      Assert.That(await Visible(otherSession_)
                    .ConfigureAwait(false),
                  Is.Empty);
    }
  }

  [Test]
  public void ARequestNotAuthenticatedByTheMiddlewareIsNotDecided()
  {
    if (RunTests)
    {
      // Credentials in the headers are not enough: without the middleware, the request would have been read before
      // its caller was authenticated
      Assert.That(() => enforcer_!.AuthorizeAsync(nameof(GrpcSessionsService),
                                                  nameof(GrpcSessionsService.GetSession),
                                                  MethodAuthorization.Rules[(nameof(GrpcSessionsService), nameof(GrpcSessionsService.GetSession))],
                                                  new GetSessionRequest
                                                  {
                                                    SessionId = aliceSession_,
                                                  },
                                                  Context(Alice)),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("UseCallerAuthentication"));
    }
  }

  [Test]
  public void ARequestWithoutCredentialIsUnauthenticated()
  {
    if (RunTests)
    {
      Assert.That(Assert.ThrowsAsync<RpcException>(() => GetSession(null,
                                                                    aliceSession_))!.StatusCode,
                  Is.EqualTo(StatusCode.Unauthenticated));
      Assert.That(audit_!.Messages,
                  Has.One.Contains("Unauthenticated"));
    }
  }

  private sealed class FixedEngine : IDecisionEngineProvider
  {
    private readonly DecisionEngine engine_;

    public FixedEngine(DecisionEngine engine)
      => engine_ = engine;

    public ValueTask<DecisionEngine> GetEngineAsync(CancellationToken cancellationToken = default)
      => ValueTask.FromResult(engine_);
  }

  private sealed class FailingEngine : IDecisionEngineProvider
  {
    public ValueTask<DecisionEngine> GetEngineAsync(CancellationToken cancellationToken = default)
      => throw new TimeoutException("The database did not answer");
  }

  private sealed class UnreachableAuthority : IAuthenticationService
  {
    public const string Message = "IDX20803: Unable to obtain configuration";

    public Task<AuthenticateResult> AuthenticateAsync(HttpContext context,
                                                      string?     scheme)
      => throw new InvalidOperationException(Message);

    public Task ChallengeAsync(HttpContext               context,
                               string?                   scheme,
                               AuthenticationProperties? properties)
      => throw new NotSupportedException();

    public Task ForbidAsync(HttpContext               context,
                            string?                   scheme,
                            AuthenticationProperties? properties)
      => throw new NotSupportedException();

    public Task SignInAsync(HttpContext               context,
                            string?                   scheme,
                            ClaimsPrincipal           principal,
                            AuthenticationProperties? properties)
      => throw new NotSupportedException();

    public Task SignOutAsync(HttpContext               context,
                             string?                   scheme,
                             AuthenticationProperties? properties)
      => throw new NotSupportedException();
  }

  private sealed class RecordingLoggerFactory : ILoggerFactory, ILogger
  {
    public List<string> Messages { get; } = new();

    public void Log<TState>(LogLevel                         logLevel,
                            EventId                          eventId,
                            TState                           state,
                            Exception?                       exception,
                            Func<TState, Exception?, string> formatter)
    {
      lock (Messages)
      {
        Messages.Add(formatter(state,
                               exception));
      }
    }

    public bool IsEnabled(LogLevel logLevel)
      => true;

    public IDisposable? BeginScope<TState>(TState state)
      where TState : notnull
      => null;

    public void Dispose()
    {
    }

    public ILogger CreateLogger(string categoryName)
      => this;

    public void AddProvider(ILoggerProvider provider)
    {
    }
  }
}
