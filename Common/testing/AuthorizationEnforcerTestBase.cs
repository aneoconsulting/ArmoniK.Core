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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Authorization;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Grpc.Core;

using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

using CreateSessionRequest = ArmoniK.Api.gRPC.V1.Sessions.CreateSessionRequest;
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

    await ResultTable.Create([Result(aliceSession_, "r-alice"), Result(otherSession_, "r-other")])
                     .ConfigureAwait(false);
    await TaskTable.CreateTasks([Task(aliceSession_, "t-alice"), Task(otherSession_, "t-other")])
                   .ConfigureAwait(false);

    var sets = AuthorizationConfiguration.PermissionSets([
                                                           new PermissionSetOption
                                                           {
                                                             Name        = "team-reader",
                                                             Permissions = ["data:read", "data:download"],
                                                           },
                                                         ]);
    var engine = DecisionEngine.Build([
                                        new GrantDefinition("team-create", "role:modelling-team", "session-create", "request.partition == \"part0\""),
                                        new GrantDefinition("team-own", "role:modelling-team", "session-own",
                                                            "session.owner == request.caller && grant.principal in session.createdUnder"),
                                        new GrantDefinition("lead-supervise", "role:lead", "session-supervise", "\"role:modelling-team\" in session.createdUnder"),
                                        new GrantDefinition("lead-read", "role:lead", "team-reader", "\"role:modelling-team\" in session.createdUnder"),
                                        new GrantDefinition("support", "role:support", "session-supervise", "\"role:modelling-team\" in session.createdUnder"),
                                        new GrantDefinition("platform", "role:platform-team", "cluster-admin", null),
                                      ],
                                      sets,
                                      out var rejected);
    Assert.That(rejected,
                Is.Empty);

    audit_ = new RecordingLoggerFactory();
    enforcer_ = new AuthorizationEnforcer(new FixedEngine(engine),
                                          new CallerResolver(new IdentityOptions()),
                                          new AuthorizationAudit(audit_),
                                          SessionTable,
                                          TaskTable,
                                          ResultTable,
                                          new Injection.Options.Submitter
                                          {
                                            DefaultPartition = "part0",
                                          },
                                          NullLogger<AuthorizationEnforcer>.Instance);
  }

  [TearDown]
  public virtual void TearDown()
  {
    SessionTable = null;
    TaskTable    = null;
    ResultTable  = null;
    RunTests     = false;
  }

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
           [],
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

  private async Task<ServerCallContext> Authorize(string? subject,
                                                  string  service,
                                                  string  method,
                                                  object  request)
  {
    var context = Context(subject);
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
  public async Task ALegacyFilterNamingNothingRequiresThePermissionAcrossTheCluster()
  {
    if (RunTests)
    {
      Assert.That(Assert.ThrowsAsync<RpcException>(() => Authorize(Alice,
                                                                   nameof(GrpcSubmitterService),
                                                                   nameof(GrpcSubmitterService.ListTasks),
                                                                   new Api.gRPC.V1.Submitter.TaskFilter()))!.StatusCode,
                  Is.EqualTo(StatusCode.PermissionDenied));
      await Authorize(Admin,
                      nameof(GrpcSubmitterService),
                      nameof(GrpcSubmitterService.ListTasks),
                      new Api.gRPC.V1.Submitter.TaskFilter())
        .ConfigureAwait(false);
      await Authorize(Alice,
                      nameof(GrpcSubmitterService),
                      nameof(GrpcSubmitterService.ListTasks),
                      new Api.gRPC.V1.Submitter.TaskFilter
                      {
                        Session = new Api.gRPC.V1.Submitter.TaskFilter.Types.IdsRequest
                                  {
                                    Ids =
                                    {
                                      aliceSession_,
                                    },
                                  },
                      })
        .ConfigureAwait(false);
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

  private sealed class RecordingLoggerFactory : ILoggerFactory, ILogger
  {
    public List<string> Messages { get; } = new();

    public void Dispose()
    {
    }

    public ILogger CreateLogger(string categoryName)
      => this;

    public void AddProvider(ILoggerProvider provider)
    {
    }

    public void Log<TState>(LogLevel                        logLevel,
                            EventId                         eventId,
                            TState                          state,
                            Exception?                      exception,
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
  }
}
