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
//   D. Brasseur       <dbrasseur@aneo.fr>
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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.Auth;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Auth.Authorization.Permissions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using Empty = ArmoniK.Api.gRPC.V1.Empty;
using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
using TaskRequest = ArmoniK.Api.gRPC.V1.TaskRequest;
using Type = System.Type;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture(AuthenticationType.DefaultAuth)]
[TestFixture(AuthenticationType.NoAuthorization)]
[TestFixture(AuthenticationType.NoAuthentication)]
[TestFixture(AuthenticationType.NoImpersonation)]
[TestFixture(AuthenticationType.NoImpersonationNoAuthorization)]
[NonParallelizable]
public class AuthenticationIntegrationTest
{
  [OneTimeSetUp]
  public void BeforeAll()
  {
    var submitter = new SimpleSubmitter();
    helper_ = new GrpcSubmitterServiceHelper(submitter,
                                             Identities.ToList(),
                                             options_!,
                                             LogLevel.Information,
                                             s =>
                                             {
                                               s.AddSingleton<ITaskTable>(new SimpleTaskTable())
                                                .AddSingleton<ISessionTable>(new SimpleSessionTable())
                                                .AddSingleton<IResultTable>(new SimpleResultTable());
                                             });
  }

  [OneTimeTearDown]
  public async Task TearDown()
  {
    if (helper_ != null)
    {
      await helper_.StopServer()
                   .ConfigureAwait(false);
      helper_.Dispose();
    }

    helper_  = null;
    options_ = null;
  }

  private const           string                  SessionId   = "MySession";
  private const           string                  TaskId      = "MyTask";
  private const           string                  ResultKey   = "ResultKey";
  private const           string                  PartitionId = "PartitionId";
  private static readonly TaskFilter              TaskFilter;
  private static readonly CreateSmallTaskRequest  CreateSmallTasksRequest;
  private static readonly CreateSessionRequest    CreateSessionRequest;
  private static readonly Session                 SessionRequest;
  private static readonly GetResultStatusRequest  GetResultStatusRequest;
  private static readonly GetTaskStatusRequest    GetTaskStatusRequest;
  private static readonly Empty                   Empty;
  private static readonly SessionFilter           SessionFilter;
  private static readonly ResultRequest           ResultRequest;
  private static readonly TaskOutputRequest       TaskOutputRequest;
  private static readonly WaitRequest             WaitRequest;
  private static readonly CreateLargeTaskRequest  CreateLargeTaskRequestInit;
  private static readonly CreateLargeTaskRequest  CreateLargeTaskRequestInitTask;
  private static readonly CreateLargeTaskRequest  CreateLargeTaskRequestPayload;
  private static readonly CreateLargeTaskRequest  CreateLargeTaskRequestPayloadComplete;
  private static readonly CreateLargeTaskRequest  CreateLargeTaskRequestLastTask;
  private static readonly CancelSessionRequest    CancelSessionRequest;
  private static readonly GetSessionRequest       GetSessionRequest;
  private static readonly ListSessionsRequest     ListSessionsRequest;
  private static readonly GetResultIdsRequest     GetResultIdsRequest;
  private static readonly GetTaskRequest          GetTaskRequest;
  private static readonly ListTasksRequest        ListTasksRequest;
  private static readonly GetOwnerTaskIdRequest   GetOwnerTaskIdRequest;
  private static readonly ListApplicationsRequest ListApplicationsRequest;
  private static readonly CancelTasksRequest      CancelTasksRequest;
  private static readonly ListResultsRequest      ListResultsRequest;
  private static readonly GetCurrentUserRequest   GetCurrentUserRequest;

  static AuthenticationIntegrationTest()
  {
    // Prepare requests
    var taskOptions = new TaskOptions
                      {
                        MaxDuration     = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                        MaxRetries      = 4,
                        Priority        = 2,
                        PartitionId     = PartitionId,
                        ApplicationName = "TestName",
                      };
    var idsrequest = new TaskFilter.Types.IdsRequest();
    idsrequest.Ids.Add(TaskId);
    TaskFilter = new TaskFilter
                 {
                   Task = idsrequest,
                 };
    CreateSmallTasksRequest = new CreateSmallTaskRequest
                              {
                                SessionId   = SessionId,
                                TaskOptions = taskOptions,
                              };
    CreateSmallTasksRequest.TaskRequests.Add(new TaskRequest
                                             {
                                               Payload = ByteString.CopyFrom("payload",
                                                                             Encoding.ASCII),
                                             });
    CreateSessionRequest = new CreateSessionRequest
                           {
                             DefaultTaskOption = taskOptions,
                             PartitionIds =
                             {
                               PartitionId,
                             },
                           };
    SessionRequest = new Session
                     {
                       Id = SessionId,
                     };
    GetResultStatusRequest = new GetResultStatusRequest
                             {
                               SessionId = SessionId,
                             };
    GetTaskStatusRequest = new GetTaskStatusRequest();
    GetTaskStatusRequest.TaskIds.Add(TaskId);
    Empty         = new Empty();
    SessionFilter = new SessionFilter();
    SessionFilter.Sessions.Add(SessionId);
    ResultRequest = new ResultRequest
                    {
                      ResultId = ResultKey,
                      Session  = SessionId,
                    };
    TaskOutputRequest = new TaskOutputRequest
                        {
                          TaskId  = TaskId,
                          Session = SessionId,
                        };
    WaitRequest = new WaitRequest
                  {
                    Filter                      = TaskFilter,
                    StopOnFirstTaskCancellation = true,
                    StopOnFirstTaskError        = true,
                  };

    CreateLargeTaskRequestInit = new CreateLargeTaskRequest
                                 {
                                   InitRequest = new CreateLargeTaskRequest.Types.InitRequest
                                                 {
                                                   SessionId   = SessionId,
                                                   TaskOptions = taskOptions,
                                                 },
                                 };
    var taskRequestHeader = new TaskRequestHeader();
    taskRequestHeader.DataDependencies.Add("dependency");
    taskRequestHeader.ExpectedOutputKeys.Add("outputKey");
    CreateLargeTaskRequestInitTask = new CreateLargeTaskRequest
                                     {
                                       InitTask = new InitTaskRequest
                                                  {
                                                    Header = taskRequestHeader,
                                                  },
                                     };
    CreateLargeTaskRequestPayload = new CreateLargeTaskRequest
                                    {
                                      TaskPayload = new DataChunk
                                                    {
                                                      Data = ByteString.CopyFrom("payload",
                                                                                 Encoding.ASCII),
                                                    },
                                    };
    CreateLargeTaskRequestPayloadComplete = new CreateLargeTaskRequest
                                            {
                                              TaskPayload = new DataChunk
                                                            {
                                                              DataComplete = true,
                                                            },
                                            };
    CreateLargeTaskRequestLastTask = new CreateLargeTaskRequest
                                     {
                                       InitTask = new InitTaskRequest
                                                  {
                                                    LastTask = true,
                                                  },
                                     };

    CancelSessionRequest = new CancelSessionRequest
                           {
                             SessionId = SessionId,
                           };

    GetSessionRequest = new GetSessionRequest
                        {
                          SessionId = SessionId,
                        };

    ListSessionsRequest = new ListSessionsRequest
                          {
                            Filter = new ListSessionsRequest.Types.Filter
                                     {
                                       SessionId = SessionId,
                                     },
                            Page     = 1,
                            PageSize = 10,
                            Sort = new ListSessionsRequest.Types.Sort
                                   {
                                     Direction = ListSessionsRequest.Types.OrderDirection.Asc,
                                     Field     = ListSessionsRequest.Types.OrderByField.SessionId,
                                   },
                          };

    GetResultIdsRequest = new GetResultIdsRequest();
    GetResultIdsRequest.TaskId.Add(TaskId);
    GetTaskRequest = new GetTaskRequest
                     {
                       TaskId = TaskId,
                     };
    ListTasksRequest = new ListTasksRequest
                       {
                         Filter = new ListTasksRequest.Types.Filter
                                  {
                                    SessionId = SessionId,
                                  },
                         Page     = 1,
                         PageSize = 10,
                         Sort = new ListTasksRequest.Types.Sort
                                {
                                  Direction = ListTasksRequest.Types.OrderDirection.Asc,
                                  Field     = ListTasksRequest.Types.OrderByField.SessionId,
                                },
                       };
    GetOwnerTaskIdRequest = new GetOwnerTaskIdRequest
                            {
                              SessionId = SessionId,
                            };
    GetOwnerTaskIdRequest.ResultId.Add(ResultKey);

    ListApplicationsRequest = new ListApplicationsRequest
                              {
                                Filter = new ListApplicationsRequest.Types.Filter
                                         {
                                           Name = taskOptions.ApplicationName,
                                         },
                                Page     = 0,
                                PageSize = 10,
                                Sort = new ListApplicationsRequest.Types.Sort
                                       {
                                         Direction = ListApplicationsRequest.Types.OrderDirection.Asc,
                                         Field     = ListApplicationsRequest.Types.OrderByField.Name,
                                       },
                              };

    CancelTasksRequest = new CancelTasksRequest();
    CancelTasksRequest.TaskIds.Add(TaskId);

    ListResultsRequest = new ListResultsRequest
                         {
                           Filter = new ListResultsRequest.Types.Filter
                                    {
                                      SessionId = SessionId,
                                    },
                           Page     = 0,
                           PageSize = 10,
                           Sort = new ListResultsRequest.Types.Sort
                                  {
                                    Direction = ListResultsRequest.Types.OrderDirection.Asc,
                                    Field     = ListResultsRequest.Types.OrderByField.Name,
                                  },
                         };
    GetCurrentUserRequest = new GetCurrentUserRequest();
  }

  public enum AuthenticationType
  {
    // Auth and Authorization
    DefaultAuth,

    // Auth Only
    NoAuthorization,

    // No Auth, No Authorization
    NoAuthentication,

    // Auth and Authorization, no impersonation
    NoImpersonation,

    // Auth, no impersonation no authorization
    NoImpersonationNoAuthorization,
  }

  private GrpcSubmitterServiceHelper? helper_;

  private          AuthenticatorOptions? options_;
  private readonly AuthenticationType    authType_;

  public AuthenticationIntegrationTest(AuthenticationType type)
  {
    TestContext.Progress.WriteLine(type);
    authType_ = type;
    options_  = new AuthenticatorOptions();
    switch (authType_)
    {
      case AuthenticationType.DefaultAuth:
        options_.CopyFrom(AuthenticatorOptions.DefaultAuth);
        break;
      case AuthenticationType.NoAuthorization:
        options_.CopyFrom(AuthenticatorOptions.DefaultAuth);
        options_.RequireAuthorization = false;
        break;
      case AuthenticationType.NoImpersonation:
        options_.CopyFrom(AuthenticatorOptions.DefaultAuth);
        options_.ImpersonationIdHeader       = "";
        options_.ImpersonationUsernameHeader = "";
        break;
      case AuthenticationType.NoImpersonationNoAuthorization:
        options_.CopyFrom(AuthenticatorOptions.DefaultAuth);
        options_.ImpersonationIdHeader       = "";
        options_.ImpersonationUsernameHeader = "";
        options_.RequireAuthorization        = false;
        break;
      case AuthenticationType.NoAuthentication:
        options_.CopyFrom(AuthenticatorOptions.DefaultNoAuth);
        break;
      default:
        throw new ArgumentException("Invalid authentication type",
                                    nameof(type));
    }

    TestContext.Progress.WriteLine(options_.ImpersonationUsernameHeader);
  }

  public enum IdentityIndex
  {
    MissingHeaders = -2,
    DoesntExist    = -1,
    AllRights      = 0,
    NoRights       = 1,
    CanImpersonate = 2,
    NoCertificate  = 3,
    SomeRights     = 4,
    OtherRights    = 5,
  }

  public enum ResultType
  {
    AlwaysTrue,
    AlwaysFalse,
    AuthorizedForSome,
  }

  public enum ImpersonationType
  {
    ImpersonateId,
    ImpersonateUsername,
    NoImpersonate,
  }

  public const string AllRightsId       = "AllRightsId";
  public const string AllRightsUsername = "AllRightsUsername";
  public const string AllRightsRole     = "AllRights";

  public static readonly MockIdentity[] Identities =
  {
    new(AllRightsId,
        AllRightsUsername,
        new[]
        {
          new MockIdentity.MockCertificate("AllRightsCN",
                                           "AllRightsFingerprint"),
        },
        new[]
        {
          AllRightsRole,
        },
        ServicesPermissions.PermissionsLists[ServicesPermissions.All],
        Authenticator.SchemeName),
    new("NoRightsId1",
        "NoRightsUsername1",
        new[]
        {
          new MockIdentity.MockCertificate("NoRightsCN",
                                           "NoRightsFingerprint"),
        },
        new[]
        {
          "AntiImpersonateRole",
        },
        Array.Empty<Permission>(),
        Authenticator.SchemeName),
    new("CanImpersonateId1",
        "CanImpersonateUsername1",
        new[]
        {
          new MockIdentity.MockCertificate("CanImpersonateCN",
                                           "CanImpersonateFingerprint"),
        },
        new[]
        {
          "CanImpersonateAllRights",
        },
        new[]
        {
          new Permission(GeneralService.Impersonate.Service,
                         GeneralService.Impersonate.Name,
                         AllRightsRole),
        },
        Authenticator.SchemeName),
    new("NoCertificateId",
        "NoCertificateUsername",
        Array.Empty<MockIdentity.MockCertificate>(),
        Array.Empty<string>(),
        Array.Empty<Permission>(),
        null),
    new("SomeRightsId",
        "SomeRightsUsername",
        new[]
        {
          new MockIdentity.MockCertificate("SomeRightsCN",
                                           "SomeRightsFingerprint"),
        },
        new[]
        {
          "SomeRights",
        },
        ServicesPermissions.PermissionsLists[ServicesPermissions.All]
                           .Where((_,
                                   index) => index % 2 == 0),
        Authenticator.SchemeName),
    new("OtherRightsId",
        "OtherRightsUsername",
        new[]
        {
          new MockIdentity.MockCertificate("OtherRightsCN",
                                           "OtherRightsFingerprint"),
        },
        new[]
        {
          "OtherRights",
        },
        ServicesPermissions.PermissionsLists[ServicesPermissions.All]
                           .Where((_,
                                   index) => index % 2 == 1),
        Authenticator.SchemeName),
  };

  public static Metadata GetHeaders(IdentityIndex     index,
                                    ImpersonationType impersonationType,
                                    IdentityIndex     impersonate)
  {
    var headers = new Metadata();
    var defaultCertificate = new MockIdentity.MockCertificate("Default",
                                                              "Default");
    if ((int)index < -1)
    {
      return headers;
    }

    headers.Add(AuthenticatorOptions.DefaultAuth.CNHeader,
                index == IdentityIndex.DoesntExist
                  ? "DoesntExistCN"
                  : Identities[(int)index]
                    .Certificates.FirstOrDefault(defaultCertificate)
                    .CN);
    headers.Add(AuthenticatorOptions.DefaultAuth.FingerprintHeader,
                index == IdentityIndex.DoesntExist
                  ? "DoesntExistFingerprint"
                  : Identities[(int)index]
                    .Certificates.FirstOrDefault(defaultCertificate)
                    .Fingerprint);
    if (impersonationType == ImpersonationType.ImpersonateId)
    {
      headers.Add(AuthenticatorOptions.DefaultAuth.ImpersonationIdHeader,
                  (int)impersonate < 0
                    ? "DoesntExist"
                    : Identities[(int)impersonate]
                      .UserId);
    }
    else if (impersonationType == ImpersonationType.ImpersonateUsername)
    {
      headers.Add(AuthenticatorOptions.DefaultAuth.ImpersonationUsernameHeader,
                  (int)impersonate < 0
                    ? "DoesntExist"
                    : Identities[(int)impersonate]
                      .UserName);
    }

    return headers;
  }

  public static object?[] GetArgs(object?           obj,
                                  IdentityIndex     identityIndex,
                                  ImpersonationType impersonationType,
                                  IdentityIndex     impersonate)
    => new[]
       {
         obj,
         GetHeaders(identityIndex,
                    impersonationType,
                    impersonate),
         null,
         new CancellationToken(),
       };

  // Identities and expectations
  private static readonly List<object[]> ParametersList = new()
                                                          {
                                                            new object[]
                                                            {
                                                              IdentityIndex.AllRights,
                                                              ResultType.AlwaysTrue,
                                                              StatusCode.OK,
                                                              IdentityIndex.AllRights,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.NoRights,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.AllRights,
                                                              ImpersonationType.ImpersonateUsername,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.NoRights,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.PermissionDenied,
                                                              IdentityIndex.NoRights,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.CanImpersonate,
                                                              ResultType.AlwaysTrue,
                                                              StatusCode.OK,
                                                              IdentityIndex.AllRights,
                                                              ImpersonationType.ImpersonateUsername,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.CanImpersonate,
                                                              ResultType.AlwaysTrue,
                                                              StatusCode.OK,
                                                              IdentityIndex.AllRights,
                                                              ImpersonationType.ImpersonateId,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.CanImpersonate,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.NoRights,
                                                              ImpersonationType.ImpersonateUsername,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.NoCertificate,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.NoCertificate,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.SomeRights,
                                                              ResultType.AuthorizedForSome,
                                                              StatusCode.PermissionDenied,
                                                              IdentityIndex.SomeRights,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.OtherRights,
                                                              ResultType.AuthorizedForSome,
                                                              StatusCode.PermissionDenied,
                                                              IdentityIndex.OtherRights,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.DoesntExist,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.DoesntExist,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.CanImpersonate,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.DoesntExist,
                                                              ImpersonationType.ImpersonateId,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.MissingHeaders,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.MissingHeaders,
                                                              ImpersonationType.NoImpersonate,
                                                            },
                                                            new object[]
                                                            {
                                                              IdentityIndex.MissingHeaders,
                                                              ResultType.AlwaysFalse,
                                                              StatusCode.Unauthenticated,
                                                              IdentityIndex.AllRights,
                                                              ImpersonationType.ImpersonateId,
                                                            },
                                                          };

  /*public static nameFromArgs(Type type,
                             string method,
                             IdentityIndex identityIndex,
                             ImpersonationType impersonationType,
                             IdentityIndex impersonate,
                             GetArgs(methodAndObject.Item3, identityIndex, impersonationType, impersonate), shouldSucceed, statusCode)*/

  public static IEnumerable GetCases(List<(Type, string, object?)> methodsAndObjects)
  {
    // Generator
    foreach (var parameters in ParametersList)
    {
      var identityIndex     = (IdentityIndex)parameters[0];
      var shouldSucceed     = (ResultType)parameters[1];
      var statusCode        = (StatusCode)parameters[2];
      var impersonate       = (IdentityIndex)parameters[3];
      var impersonationType = (ImpersonationType)parameters[4];
      foreach (var methodAndObject in methodsAndObjects)
      {
        yield return new TestCaseData(methodAndObject.Item1,
                                      methodAndObject.Item2,
                                      identityIndex,
                                      impersonationType,
                                      impersonate,
                                      GetArgs(methodAndObject.Item3,
                                              identityIndex,
                                              impersonationType,
                                              impersonate),
                                      shouldSucceed,
                                      statusCode);
      }
    }
  }

  public static readonly IReadOnlyDictionary<Type, Type> ClientServerTypeMapping = new ReadOnlyDictionary<Type, Type>(new Dictionary<Type, Type>
                                                                                                                      {
                                                                                                                        {
                                                                                                                          typeof(Api.gRPC.V1.Submitter.Submitter.
                                                                                                                            SubmitterClient),
                                                                                                                          typeof(GrpcSubmitterService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Tasks.TasksClient),
                                                                                                                          typeof(GrpcTasksService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Sessions.SessionsClient),
                                                                                                                          typeof(GrpcSessionsService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Results.ResultsClient),
                                                                                                                          typeof(GrpcResultsService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Applications.ApplicationsClient),
                                                                                                                          typeof(GrpcApplicationsService)
                                                                                                                        },
                                                                                                                      });

  public static IEnumerable GetTestCases(string suffix)
  {
    List<(Type, string, object?)> methodObjectList = new()
                                                     {
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.CancelSession), SessionRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.CancelTasks), TaskFilter),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.CountTasks), TaskFilter),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.CreateSession), CreateSessionRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.CreateSmallTasks), CreateSmallTasksRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.GetResultStatus), GetResultStatusRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.GetServiceConfiguration), Empty),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.GetTaskStatus), GetTaskStatusRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.ListSessions), SessionFilter),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.ListTasks), TaskFilter),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.TryGetTaskOutput), TaskOutputRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.WaitForAvailability), ResultRequest),
                                                       (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient),
                                                        nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.WaitForCompletion), WaitRequest),
                                                       (typeof(Sessions.SessionsClient), nameof(Sessions.SessionsClient.CancelSession), CancelSessionRequest),
                                                       (typeof(Sessions.SessionsClient), nameof(Sessions.SessionsClient.GetSession), GetSessionRequest),
                                                       (typeof(Sessions.SessionsClient), nameof(Sessions.SessionsClient.ListSessions), ListSessionsRequest),
                                                       (typeof(Applications.ApplicationsClient), nameof(Applications.ApplicationsClient.ListApplications),
                                                        ListApplicationsRequest),
                                                       (typeof(Tasks.TasksClient), nameof(Tasks.TasksClient.GetResultIds), GetResultIdsRequest),
                                                       (typeof(Tasks.TasksClient), nameof(Tasks.TasksClient.GetTask), GetTaskRequest),
                                                       (typeof(Tasks.TasksClient), nameof(Tasks.TasksClient.ListTasks), ListTasksRequest),
                                                       (typeof(Tasks.TasksClient), nameof(Tasks.TasksClient.CancelTasks), CancelTasksRequest),
                                                       (typeof(Results.ResultsClient), nameof(Results.ResultsClient.GetOwnerTaskId), GetOwnerTaskIdRequest),
                                                       (typeof(Results.ResultsClient), nameof(Results.ResultsClient.ListResults), ListResultsRequest),
                                                     };

    return GetCases(methodObjectList.Select(t => (t.Item1, t.Item2 + suffix, t.Item3))
                                    .ToList());
  }

  public static IEnumerable GetCreateLargeTaskTestCases()
  {
    var methodsAndObjects = new List<(Type, string, object?)>
                            {
                              (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient), nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.CreateLargeTasks), null),
                            };

    return GetCases(methodsAndObjects);
  }

  public static IEnumerable GetTryGetResultStreamTestCases()
  {
    var methodsAndObjects = new List<(Type, string, object?)>
                            {
                              (typeof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient), nameof(Api.gRPC.V1.Submitter.Submitter.SubmitterClient.TryGetResultStream),
                               null),
                            };

    return GetCases(methodsAndObjects);
  }

  public void TransformResult(IdentityIndex     initialUserIndex,
                              ImpersonationType impersonationType,
                              ResultType        success,
                              IdentityIndex     impersonating,
                              StatusCode        initialErrorCode,
                              out int           userIndex,
                              out ResultType    shouldSucceed,
                              out StatusCode    errorCode)
  {
    switch (authType_)
    {
      case AuthenticationType.NoAuthentication:
        userIndex     = (int)initialUserIndex;
        shouldSucceed = ResultType.AlwaysTrue;
        errorCode     = StatusCode.OK;
        break;
      case AuthenticationType.NoAuthorization:
        userIndex = impersonationType == ImpersonationType.NoImpersonate
                      ? (int)initialUserIndex
                      : (int)impersonating;
        shouldSucceed = success == ResultType.AlwaysTrue
                          ? ResultType.AlwaysTrue
                          : initialErrorCode == StatusCode.PermissionDenied || success == ResultType.AuthorizedForSome
                            ? ResultType.AlwaysTrue
                            : ResultType.AlwaysFalse;
        errorCode = initialErrorCode == StatusCode.Unauthenticated
                      ? initialErrorCode
                      : StatusCode.OK;
        break;
      case AuthenticationType.NoImpersonation:
        userIndex = (int)initialUserIndex;
        if (impersonationType == ImpersonationType.NoImpersonate)
        {
          shouldSucceed = success;
          errorCode     = initialErrorCode;
        }
        else if (initialUserIndex <= IdentityIndex.DoesntExist)
        {
          shouldSucceed = ResultType.AlwaysFalse;
          errorCode     = StatusCode.Unauthenticated;
        }
        else
        {
          if (initialErrorCode == StatusCode.Unauthenticated)
          {
            if (initialUserIndex == IdentityIndex.CanImpersonate)
            {
              shouldSucceed = ResultType.AlwaysFalse;
              errorCode     = StatusCode.PermissionDenied;
            }
            else
            {
              shouldSucceed = ResultType.AuthorizedForSome;
              errorCode     = StatusCode.PermissionDenied;
            }
          }
          else
          {
            shouldSucceed = ResultType.AuthorizedForSome;
            errorCode     = StatusCode.PermissionDenied;
          }
        }

        break;
      case AuthenticationType.NoImpersonationNoAuthorization:
        userIndex = (int)initialUserIndex;
        if (impersonationType == ImpersonationType.NoImpersonate)
        {
          shouldSucceed = success == ResultType.AlwaysTrue
                            ? ResultType.AlwaysTrue
                            : initialErrorCode == StatusCode.PermissionDenied || success == ResultType.AuthorizedForSome
                              ? ResultType.AlwaysTrue
                              : ResultType.AlwaysFalse;
          errorCode = initialErrorCode == StatusCode.Unauthenticated
                        ? initialErrorCode
                        : StatusCode.OK;
        }
        else if (initialUserIndex <= IdentityIndex.DoesntExist)
        {
          shouldSucceed = ResultType.AlwaysFalse;
          errorCode     = StatusCode.Unauthenticated;
        }
        else
        {
          shouldSucceed = ResultType.AlwaysTrue;
          errorCode     = StatusCode.OK;
        }

        break;
      default:
      case AuthenticationType.DefaultAuth:
        userIndex = impersonationType == ImpersonationType.NoImpersonate
                      ? (int)initialUserIndex
                      : (int)impersonating;
        shouldSucceed = success;
        errorCode     = initialErrorCode;
        break;
    }
  }

  [TestCaseSource(nameof(GetTestCases),
                  new object?[]
                  {
                    "",
                  })]
  public async Task AuthMatchesBehavior(Type              clientType,
                                        string            method,
                                        IdentityIndex     userIndex,
                                        ImpersonationType impersonationType,
                                        IdentityIndex     impersonating,
                                        object[]          args,
                                        ResultType        success,
                                        StatusCode        errorCode)
  {
    TransformResult(userIndex,
                    impersonationType,
                    success,
                    impersonating,
                    errorCode,
                    out var finalUserIndex,
                    out var shouldSucceed,
                    out var expectedError);
    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client = Activator.CreateInstance(clientType,
                                          channel);
    Assert.IsNotNull(client);
    Assert.IsInstanceOf<ClientBase>(client);

    var serviceName = ServicesPermissions.FromType(ClientServerTypeMapping[clientType]);

    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[finalUserIndex]
                                                                                                    .Permissions.Any(p => p.Service == serviceName && p.Name == method)))
    {
      Assert.DoesNotThrow(delegate
                          {
                            client!.GetType()
                                   .InvokeMember(method,
                                                 BindingFlags.InvokeMethod,
                                                 null,
                                                 client,
                                                 args);
                          });
    }
    else
    {
      var exception = Assert.Catch(delegate
                                   {
                                     client!.GetType()
                                            .InvokeMember(method,
                                                          BindingFlags.InvokeMethod,
                                                          null,
                                                          client,
                                                          args);
                                   });
      Assert.IsNotNull(exception);
      Assert.IsNotNull(exception!.InnerException);
      Assert.IsInstanceOf<RpcException>(exception.InnerException);
      Assert.AreEqual(expectedError,
                      ((RpcException)exception.InnerException!).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }

  [TestCaseSource(nameof(GetTestCases),
                  new object?[]
                  {
                    "Async",
                  })]
  public async Task AsyncAuthMatchesBehavior(Type              clientType,
                                             string            method,
                                             IdentityIndex     userIndex,
                                             ImpersonationType impersonationType,
                                             IdentityIndex     impersonating,
                                             object[]          args,
                                             ResultType        success,
                                             StatusCode        errorCode)
  {
    TransformResult(userIndex,
                    impersonationType,
                    success,
                    impersonating,
                    errorCode,
                    out var finalUserIndex,
                    out var shouldSucceed,
                    out var expectedError);
    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client = Activator.CreateInstance(clientType,
                                          channel);
    Assert.IsNotNull(client);
    Assert.IsInstanceOf<ClientBase>(client);

    var serviceName = ServicesPermissions.FromType(ClientServerTypeMapping[clientType]);

    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[finalUserIndex]
                                                                                                    .Permissions.Any(p => p.Service        == serviceName &&
                                                                                                                          p.Name + "Async" == method)))
    {
      Assert.DoesNotThrowAsync(delegate
                               {
                                 dynamic call = client!.GetType()
                                                       .InvokeMember(method,
                                                                     BindingFlags.InvokeMethod,
                                                                     null,
                                                                     client,
                                                                     args)!;
                                 var t = call.GetType();
                                 return t.GetProperty("ResponseAsync")
                                         .GetValue(call,
                                                   null);
                               });
    }
    else
    {
      var exception = Assert.CatchAsync(delegate
                                        {
                                          dynamic call = client!.GetType()
                                                                .InvokeMember(method,
                                                                              BindingFlags.InvokeMethod,
                                                                              null,
                                                                              client,
                                                                              args)!;
                                          var t = call.GetType();
                                          return t.GetProperty("ResponseAsync")
                                                  .GetValue(call,
                                                            null);
                                        });
      Assert.IsNotNull(exception);
      Assert.IsInstanceOf<RpcException>(exception);
      Assert.AreEqual(expectedError,
                      ((RpcException)exception!).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }

  public static async Task<CreateTaskReply> CreateLargeTask(AsyncClientStreamingCall<CreateLargeTaskRequest, CreateTaskReply> stream)
  {
    Console.WriteLine("init");
    await stream.RequestStream.WriteAsync(CreateLargeTaskRequestInit)
                .ConfigureAwait(false);
    Console.WriteLine("initTask");
    await stream.RequestStream.WriteAsync(CreateLargeTaskRequestInitTask)
                .ConfigureAwait(false);
    Console.WriteLine("payload");
    await stream.RequestStream.WriteAsync(CreateLargeTaskRequestPayload)
                .ConfigureAwait(false);
    Console.WriteLine("payloadComplete");
    await stream.RequestStream.WriteAsync(CreateLargeTaskRequestPayloadComplete)
                .ConfigureAwait(false);
    Console.WriteLine("lastTask");
    await stream.RequestStream.WriteAsync(CreateLargeTaskRequestLastTask)
                .ConfigureAwait(false);
    return await stream.ResponseAsync.ConfigureAwait(false);
  }

  [Ignore("Somehow throws a RPCException but with OK Status in pipeline. Can't reproduce locally, both in windows and wsl. Investigation ticket : #405")]
  [TestCaseSource(nameof(GetCreateLargeTaskTestCases))]
  public async Task CreateLargeTasksAuthShouldMatch(Type              clientType,
                                                    string            method,
                                                    IdentityIndex     initialUserIndex,
                                                    ImpersonationType impersonationType,
                                                    IdentityIndex     impersonating,
                                                    object[]          args,
                                                    ResultType        success,
                                                    StatusCode        initialErrorCode)
  {
    TransformResult(initialUserIndex,
                    impersonationType,
                    success,
                    impersonating,
                    initialErrorCode,
                    out var finalUserIndex,
                    out var shouldSucceed,
                    out var expectedError);
    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client      = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);
    var serviceName = ServicesPermissions.FromType(ClientServerTypeMapping[clientType]);

    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[finalUserIndex]
                                                                                                    .Permissions.Any(p => p.Service == serviceName && p.Name == method)))
    {
      Assert.DoesNotThrowAsync(async () =>
                               {
                                 var stream = client.CreateLargeTasks((Metadata)args[1]);
                                 await CreateLargeTask(stream)
                                   .ConfigureAwait(false);
                                 await stream.RequestStream.CompleteAsync()
                                             .ConfigureAwait(false);
                               });
    }
    else
    {
      var exception = Assert.CatchAsync(async () =>
                                        {
                                          var stream = client.CreateLargeTasks((Metadata)args[1]);
                                          await CreateLargeTask(stream)
                                            .ConfigureAwait(false);
                                          await stream.RequestStream.CompleteAsync()
                                                      .ConfigureAwait(false);
                                        });
      Assert.IsNotNull(exception);
      Assert.IsInstanceOf<RpcException>(exception);
      Assert.AreEqual(expectedError,
                      ((RpcException)exception!).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }

  [TestCaseSource(nameof(GetTryGetResultStreamTestCases))]
  public async Task TryGetResultStreamAuthShouldMatch(Type              clientType,
                                                      string            method,
                                                      IdentityIndex     initialUserIndex,
                                                      ImpersonationType impersonationType,
                                                      IdentityIndex     impersonating,
                                                      object[]          args,
                                                      ResultType        success,
                                                      StatusCode        initialErrorCode)
  {
    TransformResult(initialUserIndex,
                    impersonationType,
                    success,
                    impersonating,
                    initialErrorCode,
                    out var finalUserIndex,
                    out var shouldSucceed,
                    out var expectedError);
    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client      = new Api.gRPC.V1.Submitter.Submitter.SubmitterClient(channel);
    var serviceName = ServicesPermissions.FromType(ClientServerTypeMapping[clientType]);
    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[finalUserIndex]
                                                                                                    .Permissions.Any(p => p.Service == serviceName && p.Name == method)))
    {
      Assert.DoesNotThrowAsync(() => client.TryGetResultStream(ResultRequest,
                                                               (Metadata)args[1])
                                           .ResponseStream.ReadAllAsync()
                                           .ToListAsync()
                                           .AsTask());
    }
    else
    {
      var exception = Assert.CatchAsync(() => client.TryGetResultStream(ResultRequest,
                                                                        (Metadata)args[1])
                                                    .ResponseStream.ReadAllAsync()
                                                    .ToListAsync()
                                                    .AsTask());
      Assert.IsNotNull(exception);
      Assert.IsInstanceOf<RpcException>(exception);
      Assert.AreEqual(expectedError,
                      ((RpcException)exception!).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }

  public static IEnumerable GetAuthServiceTestCaseSource()
  {
    List<(Type, string, object?)> methodObjectList = new()
                                                     {
                                                       (typeof(Authentication.AuthenticationClient), nameof(Authentication.AuthenticationClient.GetCurrentUser),
                                                        GetCurrentUserRequest),
                                                     };
    return GetCases(methodObjectList);
  }

  [TestCaseSource(nameof(GetAuthServiceTestCaseSource))]
  public async Task AuthServiceShouldGiveUserInfo(Type              clientType,
                                                  string            method,
                                                  IdentityIndex     initialUserIndex,
                                                  ImpersonationType impersonationType,
                                                  IdentityIndex     impersonating,
                                                  object[]          args,
                                                  ResultType        success,
                                                  StatusCode        initialErrorCode)
  {
    TransformResult(initialUserIndex,
                    impersonationType,
                    success,
                    impersonating,
                    initialErrorCode,
                    out var finalUserIndex,
                    out var shouldSucceed,
                    out var expectedError);

    // This endpoint doesn't check permission
    if (expectedError == StatusCode.PermissionDenied)
    {
      shouldSucceed = ResultType.AlwaysTrue;
      expectedError = StatusCode.OK;
    }

    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client = Activator.CreateInstance(clientType,
                                          channel);
    Assert.IsNotNull(client);
    Assert.IsInstanceOf<ClientBase>(client);

    if (shouldSucceed is ResultType.AlwaysTrue or ResultType.AuthorizedForSome)
    {
      object? response = null;
      Assert.DoesNotThrow(delegate
                          {
                            response = client!.GetType()
                                              .InvokeMember(method,
                                                            BindingFlags.InvokeMethod,
                                                            null,
                                                            client,
                                                            args);
                          });
      Assert.IsNotNull(response);
      Assert.IsInstanceOf<GetCurrentUserResponse>(response);
      var castedResponse = (GetCurrentUserResponse)response!;
      // Check if the returned username is correct
      Assert.AreEqual(options_!.RequireAuthentication
                        ? Identities[finalUserIndex]
                          .UserName
                        : "Anonymous",
                      castedResponse.User.Username);
      // Check if the role list is empty when there is no authorization, otherwise returns the roles
      Assert.IsTrue(options_!.RequireAuthorization
                      ? !Identities[finalUserIndex]
                         .Roles.Except(castedResponse.User.Roles)
                         .Any()
                      : castedResponse.User.Roles.Count == 0);
      // Check if the permission list corresponds to 
      Assert.IsTrue(options_!.RequireAuthorization
                      ? !Identities[finalUserIndex]
                         .Permissions.Except(castedResponse.User.Permissions.Select(s => new Permission(s)))
                         .Any()
                      : !ServicesPermissions.PermissionsLists[ServicesPermissions.All]
                                            .Except(castedResponse.User.Permissions.Select(s => new Permission(s)))
                                            .Any());
    }
    else
    {
      var exception = Assert.Catch(delegate
                                   {
                                     client!.GetType()
                                            .InvokeMember(method,
                                                          BindingFlags.InvokeMethod,
                                                          null,
                                                          client,
                                                          args);
                                   });
      Assert.IsNotNull(exception);
      Assert.IsNotNull(exception!.InnerException);
      Assert.IsInstanceOf<RpcException>(exception.InnerException);
      Assert.AreEqual(expectedError,
                      ((RpcException)exception.InnerException!).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }
}
