// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.Auth;
using ArmoniK.Api.gRPC.V1.Events;

using Armonik.Api.Grpc.V1.Partitions;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;

using Armonik.Api.Grpc.V1.SortDirection;

using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;

using Armonik.Api.Grpc.V1.Versions;

using ArmoniK.Core.Base;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Auth.Authorization.Permissions;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Storage.Events;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using TaskOptions = ArmoniK.Api.gRPC.V1.TaskOptions;
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
                                             LogLevel.Warning,
                                             s =>
                                             {
                                               s.AddSingleton<ITaskTable>(new SimpleTaskTable())
                                                .AddSingleton<ISessionTable>(new SimpleSessionTable())
                                                .AddSingleton<IResultTable>(new SimpleResultTable())
                                                .AddSingleton<IPartitionTable>(new SimplePartitionTable())
                                                .AddSingleton<ITaskWatcher>(new SimpleTaskWatcher())
                                                .AddSingleton<IPushQueueStorage>(new SimplePushQueueStorage())
                                                .AddSingleton<IPullQueueStorage>(new SimplePullQueueStorage())
                                                .AddSingleton<IObjectStorage>(new SimpleObjectStorage())
                                                .AddSingleton<IResultWatcher>(new SimpleResultWatcher());
                                             },
                                             false);
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

  private const string SessionId   = "MySession";
  private const string ResultKey   = "ResultKey";
  private const string PartitionId = "PartitionId";

  static AuthenticationIntegrationTest()
  {
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

  private static readonly TaskOptions TaskOptions = new()
                                                    {
                                                      MaxDuration     = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                                                      MaxRetries      = 4,
                                                      Priority        = 2,
                                                      PartitionId     = PartitionId,
                                                      ApplicationName = "TestName",
                                                    };

  // For cases where the server enforces a specific format
  public static readonly IReadOnlyDictionary<Type, object> ManualRequests = new ReadOnlyDictionary<Type, object>(new Dictionary<Type, object>
                                                                                                                 {
                                                                                                                   {
                                                                                                                     typeof(ListApplicationsRequest),
                                                                                                                     new ListApplicationsRequest
                                                                                                                     {
                                                                                                                       Filter =
                                                                                                                         new ListApplicationsRequest.Types.Filter(),
                                                                                                                       Page     = 0,
                                                                                                                       PageSize = 1,
                                                                                                                       Sort = new ListApplicationsRequest.Types.Sort
                                                                                                                              {
                                                                                                                                Direction = SortDirection.Asc,
                                                                                                                              },
                                                                                                                     }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(ListPartitionsRequest),
                                                                                                                     new ListPartitionsRequest
                                                                                                                     {
                                                                                                                       Filter = new ListPartitionsRequest.Types.Filter(),
                                                                                                                       Page = 0,
                                                                                                                       PageSize = 1,
                                                                                                                       Sort = new ListPartitionsRequest.Types.Sort
                                                                                                                              {
                                                                                                                                Field = new PartitionField
                                                                                                                                        {
                                                                                                                                          PartitionRawField =
                                                                                                                                            PartitionRawField.Id,
                                                                                                                                        },
                                                                                                                                Direction = SortDirection.Asc,
                                                                                                                              },
                                                                                                                     }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(ListResultsRequest), new ListResultsRequest
                                                                                                                                                 {
                                                                                                                                                   Filter =
                                                                                                                                                     new
                                                                                                                                                       ListResultsRequest
                                                                                                                                                       .Types.Filter(),
                                                                                                                                                   Page     = 0,
                                                                                                                                                   PageSize = 1,
                                                                                                                                                   Sort =
                                                                                                                                                     new
                                                                                                                                                     ListResultsRequest.
                                                                                                                                                     Types.Sort
                                                                                                                                                     {
                                                                                                                                                       Field =
                                                                                                                                                         new ResultField
                                                                                                                                                         {
                                                                                                                                                           ResultRawField =
                                                                                                                                                             ResultRawField
                                                                                                                                                               .ResultId,
                                                                                                                                                         },
                                                                                                                                                       Direction =
                                                                                                                                                         SortDirection
                                                                                                                                                           .Asc,
                                                                                                                                                     },
                                                                                                                                                 }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(ListSessionsRequest), new ListSessionsRequest
                                                                                                                                                  {
                                                                                                                                                    Filter =
                                                                                                                                                      new
                                                                                                                                                        ListSessionsRequest
                                                                                                                                                        .Types.Filter(),
                                                                                                                                                    Page     = 0,
                                                                                                                                                    PageSize = 1,
                                                                                                                                                    Sort =
                                                                                                                                                      new
                                                                                                                                                      ListSessionsRequest
                                                                                                                                                      .Types.Sort
                                                                                                                                                      {
                                                                                                                                                        Field =
                                                                                                                                                          new
                                                                                                                                                          SessionField
                                                                                                                                                          {
                                                                                                                                                            SessionRawField =
                                                                                                                                                              SessionRawField
                                                                                                                                                                .SessionId,
                                                                                                                                                          },
                                                                                                                                                        Direction =
                                                                                                                                                          SortDirection
                                                                                                                                                            .Asc,
                                                                                                                                                      },
                                                                                                                                                  }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(ListTasksRequest), new ListTasksRequest
                                                                                                                                               {
                                                                                                                                                 Filter =
                                                                                                                                                   new ListTasksRequest.
                                                                                                                                                     Types.Filter(),
                                                                                                                                                 Page     = 0,
                                                                                                                                                 PageSize = 1,
                                                                                                                                                 Sort =
                                                                                                                                                   new ListTasksRequest.
                                                                                                                                                   Types.Sort
                                                                                                                                                   {
                                                                                                                                                     Field =
                                                                                                                                                       new TaskField
                                                                                                                                                       {
                                                                                                                                                         TaskSummaryField =
                                                                                                                                                           TaskSummaryField
                                                                                                                                                             .TaskId,
                                                                                                                                                       },
                                                                                                                                                     Direction =
                                                                                                                                                       SortDirection.Asc,
                                                                                                                                                   },
                                                                                                                                               }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(CreateSessionRequest),
                                                                                                                     new CreateSessionRequest
                                                                                                                     {
                                                                                                                       DefaultTaskOption = TaskOptions,
                                                                                                                     }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(CreateSmallTaskRequest),
                                                                                                                     new CreateSmallTaskRequest
                                                                                                                     {
                                                                                                                       TaskOptions = TaskOptions,
                                                                                                                     }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(CreateLargeTaskRequest),
                                                                                                                     new List<CreateLargeTaskRequest>
                                                                                                                     {
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         InitRequest =
                                                                                                                           new CreateLargeTaskRequest.Types.InitRequest
                                                                                                                           {
                                                                                                                             SessionId   = SessionId,
                                                                                                                             TaskOptions = TaskOptions,
                                                                                                                           },
                                                                                                                       },
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         InitTask = new InitTaskRequest
                                                                                                                                    {
                                                                                                                                      Header = new TaskRequestHeader
                                                                                                                                               {
                                                                                                                                                 ExpectedOutputKeys =
                                                                                                                                                 {
                                                                                                                                                   ResultKey,
                                                                                                                                                 },
                                                                                                                                               },
                                                                                                                                    },
                                                                                                                       },
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         TaskPayload = new DataChunk
                                                                                                                                       {
                                                                                                                                         Data = ByteString.Empty,
                                                                                                                                       },
                                                                                                                       },
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         TaskPayload = new DataChunk
                                                                                                                                       {
                                                                                                                                         DataComplete = true,
                                                                                                                                       },
                                                                                                                       },
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         InitTask = new InitTaskRequest
                                                                                                                                    {
                                                                                                                                      LastTask = true,
                                                                                                                                    },
                                                                                                                       },
                                                                                                                     }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(UploadResultDataRequest),
                                                                                                                     new List<UploadResultDataRequest>
                                                                                                                     {
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         Id =
                                                                                                                           new UploadResultDataRequest.Types.
                                                                                                                           ResultIdentifier
                                                                                                                           {
                                                                                                                             ResultId  = ResultKey,
                                                                                                                             SessionId = SessionId,
                                                                                                                           },
                                                                                                                       },
                                                                                                                       new()
                                                                                                                       {
                                                                                                                         DataChunk = ByteString.Empty,
                                                                                                                       },
                                                                                                                     }
                                                                                                                   },
                                                                                                                 });

  public class CasesConfig
  {
    public object? Args;
    public bool    ClientStream;
    public Type    ClientType;
    public bool    IsAsync;
    public string  Method;
    public Type    ReplyType;
    public Type    RequestType;
    public bool    ServerStream;

    public CasesConfig(Type    clientType,
                       string  method,
                       object? args,
                       Type    requestType,
                       Type    replyType,
                       bool    isAsync,
                       bool    clientStream,
                       bool    serverStream)
    {
      ClientType   = clientType;
      Method       = method;
      Args         = args;
      RequestType  = requestType;
      ReplyType    = replyType;
      IsAsync      = isAsync;
      ClientStream = clientStream;
      ServerStream = serverStream;
    }
  }

  public class CaseParameters
  {
    public object?[]         Args;
    public bool              ClientStream;
    public Type              ClientType;
    public IdentityIndex     IdentityIndex;
    public IdentityIndex     Impersonate;
    public ImpersonationType ImpersonationType;
    public bool              IsAsync;
    public string            Method;
    public bool              ServerStream;
    public ResultType        ShouldSucceed;
    public StatusCode        StatusCode;

    public CaseParameters(Type              clientType,
                          string            method,
                          object?[]         args,
                          bool              isAsync,
                          bool              clientStream,
                          bool              serverStream,
                          StatusCode        statusCode,
                          ResultType        shouldSucceed,
                          IdentityIndex     identityIndex,
                          ImpersonationType impersonationType,
                          IdentityIndex     impersonate)
    {
      ClientType        = clientType;
      Method            = method;
      Args              = args;
      IsAsync           = isAsync;
      ClientStream      = clientStream;
      ServerStream      = serverStream;
      StatusCode        = statusCode;
      ShouldSucceed     = shouldSucceed;
      IdentityIndex     = identityIndex;
      ImpersonationType = impersonationType;
      Impersonate       = impersonate;
    }

    public override string ToString()
      => $"Client: {ClientType.Name}, Method: {Method}, Status: {StatusCode}, Impersonation: {ImpersonationType} {Impersonate}, {ShouldSucceed}";
  }

  public static IEnumerable GetCases(List<CasesConfig> casesConfigs)
  {
    // Generator
    foreach (var parameters in ParametersList)
    {
      var identityIndex     = (IdentityIndex)parameters[0];
      var shouldSucceed     = (ResultType)parameters[1];
      var statusCode        = (StatusCode)parameters[2];
      var impersonate       = (IdentityIndex)parameters[3];
      var impersonationType = (ImpersonationType)parameters[4];
      foreach (var caseConfig in casesConfigs)
      {
        var caseParams = new CaseParameters(caseConfig.ClientType,
                                            caseConfig.Method,
                                            GetArgs(caseConfig.Args,
                                                    identityIndex,
                                                    impersonationType,
                                                    impersonate),
                                            caseConfig.IsAsync,
                                            caseConfig.ClientStream,
                                            caseConfig.ServerStream,
                                            statusCode,
                                            shouldSucceed,
                                            identityIndex,
                                            impersonationType,
                                            impersonate);
        yield return new TestCaseData(caseParams,
                                      Activator.CreateInstance(caseConfig.RequestType),
                                      Activator.CreateInstance(caseConfig.ReplyType)).SetName((caseConfig.IsAsync
                                                                                                 ? "Async"
                                                                                                 : "") + (caseConfig.ClientStream
                                                                                                            ? "ClientStream"
                                                                                                            : "") + (caseConfig.ServerStream
                                                                                                                       ? "ServerStream"
                                                                                                                       : "") + $"AuthShouldMatch({caseParams})");
      }
    }
  }

  public static IEnumerable GetTestReflectionCases(bool isAsync,
                                                   bool clientStream,
                                                   bool serverStream)
  {
    // Gets all the services that are subjected to authentication
    // Exclude the "General Service", as it's a fake service
    // Gets all methods where :
    // - The RequirePermission attribute is set
    // - One of the parameters is a server stream iff we are looking for server stream methods
    // - One of the parameters is a client stream iff we are looking for client stream methods
    // Then we construct a tuple for each method with :
    // - The Client type
    // - The method name
    // - The parameter of the method
    // - The Request type
    // - The Response type
    // - The Test configuration
    var methodObjectList = ServicesPermissions.Type2NameMapping.Keys.Where(k => k != typeof(GeneralService))
                                              .Select(t => (t, t.GetMethods()
                                                                .Where(mInfo => mInfo.GetCustomAttributes<RequiresPermissionAttribute>()
                                                                                     .Any())
                                                                .Where(m => !serverStream ^ m.GetParameters()
                                                                                             .Any(p => p.ParameterType.IsGenericType &&
                                                                                                       p.ParameterType.GetGenericTypeDefinition() ==
                                                                                                       typeof(IServerStreamWriter<>)))
                                                                .Where(m => !clientStream ^ m.GetParameters()
                                                                                             .Any(p => p.ParameterType.IsGenericType &&
                                                                                                       p.ParameterType.GetGenericTypeDefinition() ==
                                                                                                       typeof(IAsyncStreamReader<>)))
                                                                .ToList()))
                                              .SelectMany(tm => tm.Item2.Select(m => new CasesConfig(ServerClientTypeMapping[tm.t],
                                                                                                     m.Name + (isAsync
                                                                                                                 ? "Async"
                                                                                                                 : ""),
                                                                                                     GetParameters(m.GetParameters()[0]
                                                                                                                    .ParameterType,
                                                                                                                   clientStream),
                                                                                                     clientStream
                                                                                                       ? m.GetParameters()[0]
                                                                                                          .ParameterType.GetGenericArguments()[0]
                                                                                                       : m.GetParameters()[0]
                                                                                                          .ParameterType,
                                                                                                     serverStream
                                                                                                       ? m.GetParameters()[1]
                                                                                                          .ParameterType.GetGenericArguments()[0]
                                                                                                       : m.ReturnType.GetGenericArguments()[0],
                                                                                                     isAsync,
                                                                                                     clientStream,
                                                                                                     serverStream)))
                                              .ToList();

    return GetCases(methodObjectList);
  }

  public static object? GetParameters(Type parameterType,
                                      bool clientStream)
  {
    var finalType = parameterType.IsGenericType
                      ? parameterType.GetGenericArguments()[0]
                      : parameterType;
    return ManualRequests.ContainsKey(finalType)
             ? ManualRequests[finalType]
             : clientStream
               ? new List<object?>
                 {
                   Activator.CreateInstance(finalType),
                 }
               : Activator.CreateInstance(finalType);
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
                                                                                                                        {
                                                                                                                          typeof(Events.EventsClient),
                                                                                                                          typeof(GrpcEventsService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Partitions.PartitionsClient),
                                                                                                                          typeof(GrpcPartitionsService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Versions.VersionsClient),
                                                                                                                          typeof(GrpcVersionsService)
                                                                                                                        },
                                                                                                                        {
                                                                                                                          typeof(Authentication.AuthenticationClient),
                                                                                                                          typeof(GrpcAuthService)
                                                                                                                        },
                                                                                                                      });

  public static readonly IReadOnlyDictionary<Type, Type> ServerClientTypeMapping =
    new ReadOnlyDictionary<Type, Type>(ClientServerTypeMapping.ToDictionary(kv => kv.Value,
                                                                            kv => kv.Key));

  public static Task AsyncTestFunction(string     method,
                                       ClientBase client,
                                       object?[]  args)
  {
    dynamic call = client.GetType()
                         .InvokeMember(method,
                                       BindingFlags.InvokeMethod,
                                       null,
                                       client,
                                       args)!;
    var t = call.GetType();
    return t.GetProperty("ResponseAsync")
            .GetValue(call,
                      null);
  }

  public static Task SyncTestFunction(string     method,
                                      ClientBase client,
                                      object?[]  args)
    => Task.FromResult(client.GetType()
                             .InvokeMember(method,
                                           BindingFlags.InvokeMethod,
                                           null,
                                           client,
                                           args));

  public static async Task ClientStreamTestFunction<TRequest, TReply>(string     method,
                                                                      ClientBase client,
                                                                      object?[]  args)
  {
    var stream = client.GetType()
                       .InvokeMember(method,
                                     BindingFlags.InvokeMethod,
                                     null,
                                     client,
                                     new[]
                                     {
                                       args[1],
                                       null,
                                       CancellationToken.None,
                                     }) as AsyncClientStreamingCall<TRequest, TReply>;
    foreach (var o in (IEnumerable)args[0]!)
    {
      await stream!.RequestStream.WriteAsync((TRequest)o)
                   .ConfigureAwait(false);
    }

    await stream!.RequestStream.CompleteAsync()
                 .ConfigureAwait(false);
    await stream.ResponseAsync.ConfigureAwait(false);
  }

  public static async Task ServerStreamTestFunction<TReply>(string     method,
                                                            ClientBase client,
                                                            object?[]  args)
    => await (client.GetType()
                    .InvokeMember(method,
                                  BindingFlags.InvokeMethod,
                                  null,
                                  client,
                                  args) as AsyncServerStreamingCall<TReply>)!.ResponseStream.MoveNext(CancellationToken.None)
                                                                             .ConfigureAwait(false);

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

  [NonParallelizable]
  [TestCaseSource(nameof(GetTestReflectionCases),
                  new object?[]
                  {
                    false,
                    false,
                    false,
                  })]
  [TestCaseSource(nameof(GetTestReflectionCases),
                  new object?[]
                  {
                    true,
                    false,
                    false,
                  })]
  [TestCaseSource(nameof(GetTestReflectionCases),
                  new object?[]
                  {
                    false,
                    true,
                    false,
                  })]
  [TestCaseSource(nameof(GetTestReflectionCases),
                  new object?[]
                  {
                    false,
                    false,
                    true,
                  })]
  public void AuthenticationShouldMatch<TRequest, TReply>(CaseParameters parameters,
                                                          TRequest       requestExample,
                                                          TReply         replyExample)
  {
    TransformResult(parameters.IdentityIndex,
                    parameters.ImpersonationType,
                    parameters.ShouldSucceed,
                    parameters.Impersonate,
                    parameters.StatusCode,
                    out var finalUserIndex,
                    out var shouldSucceed,
                    out var expectedError);
    var channel = helper_!.CreateChannel()
                          .Result;
    var client = Activator.CreateInstance(parameters.ClientType,
                                          channel);
    Assert.IsNotNull(client);
    Assert.IsInstanceOf<ClientBase>(client);

    async Task TestFunction()
    {
      if (parameters.IsAsync)
      {
        await AsyncTestFunction(parameters.Method,
                                (ClientBase)client!,
                                parameters.Args)
          .ConfigureAwait(false);
      }
      else if (parameters.ClientStream)
      {
        await ClientStreamTestFunction<TRequest, TReply>(parameters.Method,
                                                         (ClientBase)client!,
                                                         parameters.Args)
          .ConfigureAwait(false);
      }
      else if (parameters.ServerStream)
      {
        await ServerStreamTestFunction<TReply>(parameters.Method,
                                               (ClientBase)client!,
                                               parameters.Args)
          .ConfigureAwait(false);
      }
      else
      {
        await SyncTestFunction(parameters.Method,
                               (ClientBase)client!,
                               parameters.Args)
          .ConfigureAwait(false);
      }
    }

    var serviceName = ServicesPermissions.FromType(ClientServerTypeMapping[parameters.ClientType]);

    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[finalUserIndex]
                                                                                                    .Permissions.Any(p => p.Service == serviceName && p.Name +
                                                                                                                          (parameters.IsAsync
                                                                                                                             ? "Async"
                                                                                                                             : "") == parameters.Method)))
    {
      Assert.DoesNotThrowAsync(TestFunction);
    }
    else
    {
      var exception = Assert.CatchAsync(TestFunction);
      Assert.IsNotNull(exception);
      var finalException = parameters.IsAsync || parameters.ClientStream || parameters.ServerStream
                             ? exception
                             : exception!.InnerException;
      Assert.IsNotNull(finalException);
      Assert.IsInstanceOf<RpcException>(finalException);
      Assert.AreEqual(expectedError,
                      ((RpcException)finalException!).StatusCode);
    }

    helper_.DeleteChannel(channel)
           .Wait();
  }

  public static IEnumerable GetAuthServiceTestCaseSource()
  {
    List<CasesConfig> methodObjectList = new()
                                         {
                                           new CasesConfig(typeof(Authentication.AuthenticationClient),
                                                           nameof(Authentication.AuthenticationClient.GetCurrentUser),
                                                           new GetCurrentUserRequest(),
                                                           typeof(GetCurrentUserRequest),
                                                           typeof(GetCurrentUserResponse),
                                                           false,
                                                           false,
                                                           false),
                                         };
    return GetCases(methodObjectList);
  }

  [TestCaseSource(nameof(GetAuthServiceTestCaseSource))]
  public async Task AuthServiceShouldGiveUserInfo(CaseParameters parameters,
                                                  object         _,
                                                  object         __)
  {
    TransformResult(parameters.IdentityIndex,
                    parameters.ImpersonationType,
                    parameters.ShouldSucceed,
                    parameters.Impersonate,
                    parameters.StatusCode,
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
    var client = Activator.CreateInstance(parameters.ClientType,
                                          channel);
    Assert.IsNotNull(client);
    Assert.IsInstanceOf<ClientBase>(client);

    if (shouldSucceed is ResultType.AlwaysTrue or ResultType.AuthorizedForSome)
    {
      object? response = null;
      Assert.DoesNotThrow(delegate
                          {
                            response = client!.GetType()
                                              .InvokeMember(parameters.Method,
                                                            BindingFlags.InvokeMethod,
                                                            null,
                                                            client,
                                                            parameters.Args);
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
      // Check if the permission list corresponds to the identity's permissions
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
                                            .InvokeMember(parameters.Method,
                                                          BindingFlags.InvokeMethod,
                                                          null,
                                                          client,
                                                          parameters.Args);
                                   });
      Assert.IsNotNull(exception);
      Assert.IsNotNull(exception!.InnerException);
      Assert.IsInstanceOf<RpcException>(exception.InnerException);
      Assert.AreEqual(expectedError,
                      ((RpcException)exception.InnerException!).StatusCode);
    }

    await helper_.DeleteChannel(channel)
                 .ConfigureAwait(false);
  }
}
