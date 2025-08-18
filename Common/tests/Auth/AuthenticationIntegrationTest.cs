// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Api.gRPC.V1.Auth;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Sessions;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Api.gRPC.V1.Versions;
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

using JetBrains.Annotations;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using NUnit.Framework;

using CreateSessionRequest = ArmoniK.Api.gRPC.V1.Submitter.CreateSessionRequest;
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
                                                .AddSingleton<IResultWatcher>(new SimpleResultWatcher())
                                                .AddSingleton(new Injection.Options.Submitter
                                                              {
                                                                DefaultPartition = "defaultPartition",
                                                                MaxErrorAllowed  = 5,
                                                              });
                                             },
                                             false);
  }

  [OneTimeTearDown]
  public async Task TearDown()
  {
    if (helper_ is not null)
    {
      await helper_.StopServer()
                   .ConfigureAwait(false);
      helper_.Dispose();
    }

    helper_  = null;
    options_ = null;
  }

  [SetUp]
  public void BeforeEach()
    => SingleThreadSemaphore.Wait();

  [TearDown]
  public void AfterEach()
    => SingleThreadSemaphore.Release();

  private const string SessionId   = "MySession";
  private const string ResultKey   = "ResultKey";
  private const string PartitionId = "PartitionId";

  static AuthenticationIntegrationTest()
  {
  }

  public enum AuthenticationType
  {
    /// <summary>
    ///   Auth and Authorization
    /// </summary>
    DefaultAuth,

    /// <summary>
    ///   Auth Only
    /// </summary>
    NoAuthorization,

    /// <summary>
    ///   No Auth, No Authorization
    /// </summary>
    NoAuthentication,

    /// <summary>
    ///   Auth and Authorization, no impersonation
    /// </summary>
    NoImpersonation,

    /// <summary>
    ///   Auth, no impersonation no authorization
    /// </summary>
    NoImpersonationNoAuthorization,
  }

  private GrpcSubmitterServiceHelper? helper_;

  private          AuthenticatorOptions? options_;
  private readonly AuthenticationType    authType_;

  /// <summary>
  ///   Creates the test and changes the options to match desired behavior
  /// </summary>
  /// <param name="type">Type of authentication</param>
  /// <exception cref="ArgumentException">If the authentication type doesn't exist</exception>
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

  /// <summary>
  ///   Enum specifying the index of the different user cases
  /// </summary>
  public enum IdentityIndex
  {
    /// <summary>
    ///   Request has been sent without headers
    /// </summary>
    MissingHeaders = -2,

    /// <summary>
    ///   The user doesn't exist
    /// </summary>
    DoesntExist = -1,

    /// <summary>
    ///   The user has all permissions
    /// </summary>
    AllRights = 0,

    /// <summary>
    ///   The user has no permissions
    /// </summary>
    NoRights = 1,

    /// <summary>
    ///   The user has the permission to impersonate
    /// </summary>
    CanImpersonate = 2,

    /// <summary>
    ///   The user has no valid certificate
    /// </summary>
    NoCertificate = 3,

    /// <summary>
    ///   The user has half of the permissions
    /// </summary>
    SomeRights = 4,

    /// <summary>
    ///   The user has the other half of the permissions
    /// </summary>
    OtherRights = 5,
  }

  /// <summary>
  ///   Defines the expected behavior
  /// </summary>
  public enum ResultType
  {
    /// <summary>
    ///   User is always authorized
    /// </summary>
    AlwaysTrue,

    /// <summary>
    ///   User is never authorized
    /// </summary>
    AlwaysFalse,

    /// <summary>
    ///   User is only authorized for the permissions they have
    /// </summary>
    AuthorizedForSome,
  }

  /// <summary>
  ///   Type of impersonation
  /// </summary>
  public enum ImpersonationType
  {
    /// <summary>
    ///   Impersonate using user id
    /// </summary>
    ImpersonateId,

    /// <summary>
    ///   Impersonate using user name
    /// </summary>
    ImpersonateUsername,

    /// <summary>
    ///   Do not impersonate
    /// </summary>
    NoImpersonate,
  }

  public const string AllRightsId       = "AllRightsId";
  public const string AllRightsUsername = "AllRightsUsername";
  public const string AllRightsRole     = "AllRights";

  /// <summary>
  ///   Definitions of the fake users
  /// </summary>
  public static readonly MockIdentity[] Identities =
  {
    // All rights
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
    // No Rights
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
    // Can impersonate
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
    // Has no certificate
    new("NoCertificateId",
        "NoCertificateUsername",
        Array.Empty<MockIdentity.MockCertificate>(),
        Array.Empty<string>(),
        Array.Empty<Permission>(),
        null),
    // Has half of the permissions
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
    // Has the other half of the permissions
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

  /// <summary>
  ///   Gets the metadata headers used to simulate an actual web call
  /// </summary>
  /// <param name="index">Identity to use</param>
  /// <param name="impersonationType">User wants to impersonate</param>
  /// <param name="impersonate">Who this user wants to impersonate</param>
  /// <returns>Corresponding headers</returns>
  public static Metadata GetHeaders(IdentityIndex     index,
                                    ImpersonationType impersonationType,
                                    IdentityIndex     impersonate)
  {
    var headers = new Metadata();
    var defaultCertificate = new MockIdentity.MockCertificate("Default",
                                                              "Default");
    if ((int)index < -1)
    {
      // Missing headers case
      return headers;
    }

    headers.Add(AuthenticatorOptions.DefaultAuth.CNHeader,
                index == IdentityIndex.DoesntExist
                  ? "DoesntExistCN"
                  : Identities[(int)index]
                    .Certificates.FirstOrDefault(defaultCertificate)
                    .Cn);
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

  /// <summary>
  ///   Get the invocation arguments for reflection from the parameters
  /// </summary>
  /// <param name="obj">Method call parameter</param>
  /// <param name="identityIndex">Initial identity</param>
  /// <param name="impersonationType">Type of impersonation</param>
  /// <param name="impersonate">Identity to impersonate</param>
  /// <returns></returns>
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

  /// <summary>
  ///   Identities and expectations
  /// </summary>
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

  /// <summary>
  ///   Default task options
  /// </summary>
  private static readonly TaskOptions TaskOptions = new()
                                                    {
                                                      MaxDuration     = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                                                      MaxRetries      = 4,
                                                      Priority        = 2,
                                                      PartitionId     = PartitionId,
                                                      ApplicationName = "TestName",
                                                    };

  /// <summary>
  ///   For cases where the server enforces a specific format
  /// </summary>
  public static readonly IReadOnlyDictionary<Type, object> ManualRequests = new ReadOnlyDictionary<Type, object>(new Dictionary<Type, object>
                                                                                                                 {
                                                                                                                   {
                                                                                                                     typeof(CreateSessionRequest),
                                                                                                                     new CreateSessionRequest
                                                                                                                     {
                                                                                                                       DefaultTaskOption = TaskOptions,
                                                                                                                     }
                                                                                                                   },
                                                                                                                   {
                                                                                                                     typeof(Api.gRPC.V1.Sessions.CreateSessionRequest),
                                                                                                                     new Api.gRPC.V1.Sessions.CreateSessionRequest
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
                                                                                                                   {
                                                                                                                     typeof(TaskFilter), new TaskFilter
                                                                                                                                         {
                                                                                                                                           Session =
                                                                                                                                             new TaskFilter.Types.
                                                                                                                                             IdsRequest
                                                                                                                                             {
                                                                                                                                               Ids =
                                                                                                                                               {
                                                                                                                                                 SessionId,
                                                                                                                                               },
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

  /// <summary>
  ///   Returns a IEnumerable of test cases with the necessary parameters
  /// </summary>
  /// <param name="casesConfigs">List of case configurations</param>
  /// <returns></returns>
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
                                      //The 2 objects below are used to for the test case to use the right generic types
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

  /// <summary>
  ///   Get the cases matching the conditions
  /// </summary>
  /// <param name="isAsync">Whether the cases should be Async calls</param>
  /// <param name="clientStream">Whether the cases should be for methods with a client stream</param>
  /// <param name="serverStream">Whether the cases should be for methods with a server stream</param>
  /// <returns>IEnumerable of test cases</returns>
  public static IEnumerable GetTestReflectionCases(bool isAsync,
                                                   bool clientStream,
                                                   bool serverStream)
  {
    // Gets all the services that are subjected to authentication
    var methodObjectList = ServicesPermissions.Type2NameMapping.Keys
                                              // Exclude the "General Service", as it's a fake service
                                              .Where(k => k != typeof(GeneralService))
                                              // Gets all methods where :
                                              .Select(t => (t, t.GetMethods()
                                                                // - The RequirePermission attribute is set
                                                                .Where(mInfo => mInfo.GetCustomAttributes<RequiresPermissionAttribute>()
                                                                                     .Any())
                                                                // - One of the parameters is a server stream iff we are looking for server stream methods
                                                                .Where(m => !serverStream ^ m.GetParameters()
                                                                                             .Any(p => p.ParameterType.IsGenericType &&
                                                                                                       p.ParameterType.GetGenericTypeDefinition() ==
                                                                                                       typeof(IServerStreamWriter<>)))
                                                                // - One of the parameters is a client stream iff we are looking for client stream methods
                                                                .Where(m => !clientStream ^ m.GetParameters()
                                                                                             .Any(p => p.ParameterType.IsGenericType &&
                                                                                                       p.ParameterType.GetGenericTypeDefinition() ==
                                                                                                       typeof(IAsyncStreamReader<>)))
                                                                .ToList()))
                                              // Then we construct a CaseConfig for each method with :
                                              .SelectMany(tm => tm.Item2.Select(m => new CasesConfig(
                                                                                                     // - The Client type
                                                                                                     ServerClientTypeMapping[tm.t],
                                                                                                     // - The method name
                                                                                                     m.Name + (isAsync
                                                                                                                 ? "Async"
                                                                                                                 : ""),
                                                                                                     // - The parameter of the method
                                                                                                     GetParameters(m.GetParameters()[0]
                                                                                                                    .ParameterType,
                                                                                                                   clientStream),
                                                                                                     // - The Request type
                                                                                                     clientStream
                                                                                                       ? m.GetParameters()[0]
                                                                                                          .ParameterType.GetGenericArguments()[0]
                                                                                                       : m.GetParameters()[0]
                                                                                                          .ParameterType,
                                                                                                     // - The Response type
                                                                                                     serverStream
                                                                                                       ? m.GetParameters()[1]
                                                                                                          .ParameterType.GetGenericArguments()[0]
                                                                                                       : m.ReturnType.GetGenericArguments()[0],
                                                                                                     // - The Test configuration
                                                                                                     isAsync,
                                                                                                     clientStream,
                                                                                                     serverStream)))
                                              .ToList();
    // Transform the configs into test cases
    return GetCases(methodObjectList);
  }

  /// <summary>
  ///   Gets the parameter of the method
  /// </summary>
  /// <param name="parameterType">Type of the parameter</param>
  /// <param name="clientStream">True if a client stream is expected</param>
  /// <returns>The parameter of the method</returns>
  public static object? GetParameters(Type parameterType,
                                      bool clientStream)
  {
    // If the parameter is a generic type (like for client streams) then we get the generic type argument
    var finalType = parameterType.IsGenericType
                      ? parameterType.GetGenericArguments()[0]
                      : parameterType;
    // Create the request(s) if it's not a manually created one available in the list of manual requests
    return ManualRequests.ContainsKey(finalType)
             ? ManualRequests[finalType]
             : clientStream
               ? new List<object?>
                 {
                   Activator.CreateInstance(finalType),
                 }
               : Activator.CreateInstance(finalType);
  }

  /// <summary>
  ///   Mapping between the client type and the server type
  /// </summary>
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

  /// <summary>
  ///   Mapping between the server type and the client type
  /// </summary>
  public static readonly IReadOnlyDictionary<Type, Type> ServerClientTypeMapping =
    new ReadOnlyDictionary<Type, Type>(ClientServerTypeMapping.ToDictionary(kv => kv.Value,
                                                                            kv => kv.Key));

  /// <summary>
  ///   Function used to test async unary-unary functions
  /// </summary>
  /// <param name="method">Method name</param>
  /// <param name="client">Client to use</param>
  /// <param name="args">Method arguments</param>
  /// <returns></returns>
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

  /// <summary>
  ///   Function used to test synchronous unary-unary functions
  /// </summary>
  /// <param name="method">Method name</param>
  /// <param name="client">Client to use</param>
  /// <param name="args">Method arguments</param>
  /// <returns></returns>
  public static Task SyncTestFunction(string     method,
                                      ClientBase client,
                                      object?[]  args)
    => Task.FromResult(client.GetType()
                             .InvokeMember(method,
                                           BindingFlags.InvokeMethod,
                                           null,
                                           client,
                                           args));

  private static readonly SemaphoreSlim SingleThreadSemaphore = new(1,
                                                                    1);

  /// <summary>
  ///   Function used to test async clientStream-unary functions
  /// </summary>
  /// <typeparam name="TRequest">Request type</typeparam>
  /// <typeparam name="TReply">Reply type</typeparam>
  /// <param name="method">Method name</param>
  /// <param name="client">Client to use</param>
  /// <param name="args">Method arguments</param>
  /// <returns></returns>
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

  /// <summary>
  ///   Function used to test async unary-serverStream functions
  /// </summary>
  /// <typeparam name="TReply">Reply type</typeparam>
  /// <param name="method">Method name</param>
  /// <param name="client">Client to use</param>
  /// <param name="args">Method arguments</param>
  /// <returns></returns>
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

  /// <summary>
  ///   Function used to transform the expected test result to the proper expectation based on the non-static options
  ///   This is necessary because the test cases have to be static
  /// </summary>
  /// <param name="initialUserIndex">Initial user</param>
  /// <param name="impersonationType">Type of impersionation</param>
  /// <param name="success">Initial success expectation</param>
  /// <param name="impersonating">Who the user wants to impersonate</param>
  /// <param name="initialErrorCode">Initially expected error code</param>
  /// <param name="userIndex">Output of the final user index</param>
  /// <param name="shouldSucceed">Output of the expected success</param>
  /// <param name="errorCode">Output of the expected error code</param>
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
      // When no authentication is required, all calls should succeed
      case AuthenticationType.NoAuthentication:
        userIndex     = (int)initialUserIndex;
        shouldSucceed = ResultType.AlwaysTrue;
        errorCode     = StatusCode.OK;
        break;
      // When no authorization is required, all successfully authenticated user succeeds
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
      // Authorization and authentication are required but no impersonation is done
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
      //Authentication is required, but impersonation is not done
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
      //Normal case : authentication and authorization are required, impersonation is authorized
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
  [PublicAPI] // removes a warning about unused parameter
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
  [SuppressMessage("Style",
                   "IDE0060:Remove unused parameter",
                   Justification = "Required for TestCaseSource")]
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

    var serviceName = ServicesPermissions.FromType(ClientServerTypeMapping[parameters.ClientType]);

    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[finalUserIndex]
                                                                                                    .Permissions.Any(p => p.Service == serviceName && p.Name +
                                                                                                                          (parameters.IsAsync
                                                                                                                             ? "Async"
                                                                                                                             : "") == parameters.Method)))
    {
      // TODO: FIX ME => The RPCException with the OK status only happens in the CI, and it's random. I don't know why
      RpcException? exception = null;
      try
      {
        TestFunction()
          .Wait();
      }
      catch (Exception ex)
      {
        Assert.That(ex.InnerException,
                    Is.Not.Null);
        Assert.That(ex.InnerException,
                    Is.InstanceOf<RpcException>());
        exception = (RpcException)ex.InnerException!;
      }

      // This handles issues when the machine is too slow and grpc gives a RpcException with no error
      Assert.That(exception,
                  Is.Null.Or.Property("StatusCode")
                    .EqualTo(StatusCode.OK));
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

    GrpcSubmitterServiceHelper.DeleteChannel(channel)
                              .Wait();
    return;

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
  }

  /// <summary>
  ///   Test case for the auth service get user
  /// </summary>
  /// <returns></returns>
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

  [PublicAPI]
  [TestCaseSource(nameof(GetAuthServiceTestCaseSource))]
  [SuppressMessage("Style",
                   "IDE0060:Remove unused parameter",
                   Justification = "Required for reflexion")]
  public async Task AuthServiceShouldGiveUserInfo(CaseParameters parameters,
                                                  object         exampleRequest,
                                                  object         exampleReply)
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

    await GrpcSubmitterServiceHelper.DeleteChannel(channel)
                                    .ConfigureAwait(false);
  }
}
