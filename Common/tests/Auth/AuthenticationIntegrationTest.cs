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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using JetBrains.Annotations;

using NUnit.Framework;

using SubmitterClient = ArmoniK.Api.gRPC.V1.Submitter.Submitter.SubmitterClient;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture(AuthenticationType.DefaultAuth)]
[TestFixture(AuthenticationType.NoAuthorization)]
[TestFixture(AuthenticationType.NoAuthentication)]
[TestFixture(AuthenticationType.NoImpersonation)]
[TestFixture(AuthenticationType.NoImpersonationNoAuthorization)]
public class AuthenticationIntegrationTest
{
  public enum AuthenticationType
  {
    DefaultAuth,
    NoAuthorization,
    NoAuthentication,
    NoImpersonation,
    NoImpersonationNoAuthorization,
  }

  private AuthenticatorOptions options_;
  private AuthenticationType   authType_;
  public AuthenticationIntegrationTest(AuthenticationType type)
  {
    authType_ = type;
    switch (type)
    {
      case AuthenticationType.DefaultAuth:
        options_ = AuthenticatorOptions.Default;
        break;
      case AuthenticationType.NoAuthorization:
        options_                      = AuthenticatorOptions.Default;
        options_.RequireAuthorization = false;
        break;
      case AuthenticationType.NoImpersonation:
        options_                             = AuthenticatorOptions.Default;
        options_.ImpersonationIdHeader       = null;
        options_.ImpersonationUsernameHeader = null;
        break;
      case AuthenticationType.NoImpersonationNoAuthorization:
        options_                             = AuthenticatorOptions.Default;
        options_.ImpersonationIdHeader       = null;
        options_.ImpersonationUsernameHeader = null;
        options_.RequireAuthorization       = false;
        break;
      case AuthenticationType.NoAuthentication:
        options_ = AuthenticatorOptions.DefaultNoAuth;
        break;
      default:
        throw new ArgumentException();
    }
  }
  public enum IdentityIndex
  {
    DoesntExist    =-1,
    AllRights      = 0,
    NoRights       = 1,
    CanImpersonate = 2,
    NoCertifcate   = 3,
    SomeRights     = 4,
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

  public static MockIdentity[] Identities =
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
        Permissions.PermissionList,
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
        Array.Empty<Permissions.Permission>(),
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
          new Permissions.Permission(Permissions.Impersonate.Prefix,
                                     Permissions.Impersonate.Name,
                                     AllRightsRole),
        },
        Authenticator.SchemeName),
    new("NoCertificateId",
        "NoCertificateUsername",
        Array.Empty<MockIdentity.MockCertificate>(),
        Array.Empty<string>(),
        Array.Empty<Permissions.Permission>(),
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
        Permissions.PermissionList.Where((_,
                                          index) => index % 2 == 0),
        Authenticator.SchemeName),
  };

  [CanBeNull]
  private GrpcSubmitterServiceHelper helper_;

  [OneTimeTearDown]
  public async Task TearDown()
  {
    await helper_.StopServer()
                 .ConfigureAwait(false);
    helper_ = null;
  }

  public static Metadata GetHeaders(IdentityIndex      index,
                                    ImpersonationType impersonationType,
                                    [CanBeNull] string impersonate)
  {
    var headers = new Metadata();
    var defaultCertificate = new MockIdentity.MockCertificate("Default",
                                                              "Default");
    if (index < 0)
      return headers;
    headers.Add(AuthenticatorOptions.Default.CNHeader!,
                Identities[(int)index]
                  .Certificates.FirstOrDefault(defaultCertificate)
                  .CN);
    headers.Add(AuthenticatorOptions.Default.FingerprintHeader!,
                Identities[(int)index]
                  .Certificates.FirstOrDefault(defaultCertificate)
                  .Fingerprint);
    if (impersonationType == ImpersonationType.ImpersonateId)
    {
      headers.Add(AuthenticatorOptions.Default.ImpersonationIdHeader!,
                  impersonate!);
    }else if (impersonationType == ImpersonationType.ImpersonateUsername)
    {
      headers.Add(AuthenticatorOptions.Default.ImpersonationUsernameHeader!,
                  impersonate!);
    }

    return headers;
  }

  public static object[] GetArgs(object             obj,
                                 IdentityIndex      identityIndex,
                                 ImpersonationType impersonationType,
                                 [CanBeNull] string impersonate)
  {
    return new[]
           {
             obj,
             GetHeaders(identityIndex,
                        impersonationType,
                        impersonate),
             null,
             new CancellationToken(),
           };
  }

  public static IEnumerable GetTestCases()
  {
    // Constants
    const string sessionId = "MySession";
    const string taskId    = "MyTask";
    var taskOptions = new TaskOptions
                      {
                        MaxDuration = Duration.FromTimeSpan(TimeSpan.FromSeconds(10)),
                        MaxRetries  = 4,
                        Priority    = 2,
                      };
    var idsrequest = new TaskFilter.Types.IdsRequest();
    idsrequest.Ids.Add(taskId);
    var taskFilter = new TaskFilter
                     {
                       Task = idsrequest,
                     };
    var createSmallTasksRequest = new CreateSmallTaskRequest
                                  {
                                    SessionId   = sessionId,
                                    TaskOptions = taskOptions
                                  };
    createSmallTasksRequest.TaskRequests.Add(new TaskRequest
                                             {Id=taskId,Payload = ByteString.CopyFrom("payload", Encoding.ASCII)});

    // Parameters
    var parametersList = new List<object[]>
                     {
                       new object[]
                       {
                         IdentityIndex.AllRights,
                         ResultType.AlwaysTrue,
                         StatusCode.OK,
                         null,
                         ImpersonationType.NoImpersonate,
                       },
                       new object[]
                       {
                         IdentityIndex.AllRights,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         "NoRightsUsername1",
                         ImpersonationType.ImpersonateUsername,
                       },
                       new object[]
                       {
                         IdentityIndex.NoRights,
                         ResultType.AlwaysFalse,
                         StatusCode.PermissionDenied,
                         null,
                         ImpersonationType.NoImpersonate,
                       },
                       new object[]
                       {
                         IdentityIndex.CanImpersonate,
                         ResultType.AlwaysTrue,
                         StatusCode.OK,
                         AllRightsUsername,
                         ImpersonationType.ImpersonateUsername,
                       },
                       new object[]
                       {
                         IdentityIndex.CanImpersonate,
                         ResultType.AlwaysTrue,
                         StatusCode.OK,
                         AllRightsId,
                         ImpersonationType.ImpersonateId,
                       },
                       new object[]
                       {
                         IdentityIndex.CanImpersonate,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         "NoRightsUsername1",
                         ImpersonationType.ImpersonateUsername,
                       },
                       new object[]
                       {
                         IdentityIndex.NoCertifcate,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         null,
                         ImpersonationType.NoImpersonate,
                       },
                       new object[]
                       {
                         IdentityIndex.SomeRights,
                         ResultType.AuthorizedForSome,
                         StatusCode.PermissionDenied,
                         null,
                         ImpersonationType.NoImpersonate,
                       },
                       new object[]
                       {
                         IdentityIndex.DoesntExist,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         null,
                         ImpersonationType.NoImpersonate,
                       },
                     };

    // Generator
    foreach(var parameters in parametersList)
    {
      var identityIndex     = (IdentityIndex)parameters[0];
      var shouldSucceed     = (ResultType)parameters[1];
      var statusCode        = (StatusCode)parameters[2];
      var impersonate       = (string)parameters[3];
      var impersonationType = (ImpersonationType) parameters[4];
      yield return (nameof(SubmitterClient.CreateSession), identityIndex, impersonationType, impersonate, GetArgs(new CreateSessionRequest
                                                                                             {
                                                                                               Id                = sessionId,
                                                                                               DefaultTaskOption = taskOptions,
                                                                                             },
                                                                                             identityIndex,
                                                                                             impersonationType,
                                                                                             impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.CancelSession), identityIndex, impersonationType, impersonate, GetArgs(new Session
                                                                                                                 {
                                                                                                                   Id = sessionId,
                                                                                                                 },
                                                                                                                 identityIndex,
                                                                                                                 impersonationType,
                                                                                                                 impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.CountTasks), identityIndex, impersonationType, impersonate, GetArgs(taskFilter,
                                                                                                              identityIndex,
                                                                                                              impersonationType,
                                                                                                              impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.CancelTasks), identityIndex, impersonationType, impersonate, GetArgs(taskFilter,
                                                                                                               identityIndex,
                                                                                                               impersonationType,
                                                                                                               impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.CreateSmallTasks), identityIndex, impersonationType, impersonate, GetArgs(createSmallTasksRequest,
                                                                                                                    identityIndex,
                                                                                                                    impersonationType,
                                                                                                                    impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.GetResultStatus), identityIndex, impersonationType, impersonate, GetArgs(new GetResultStatusRequest
                                                                                                                   {
                                                                                                                     SessionId = sessionId,
                                                                                                                   },
                                                                                                                   identityIndex,
                                                                                                                   impersonationType,
                                                                                                                   impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.ListTasks), identityIndex, impersonationType, impersonate, GetArgs(taskFilter,
                                                                                                             identityIndex,
                                                                                                             impersonationType,
                                                                                                             impersonate), shouldSucceed, statusCode);
    }
    
  }

  [OneTimeSetUp]
  public void BeforeAll()
  {
    var submitter = new SimpleSubmitter();
    helper_ = new GrpcSubmitterServiceHelper(submitter,
                                             Identities.ToList(),
                                             options_);
  }

  public void TransformResult(
    (string method, IdentityIndex userIndex, ImpersonationType impersonationType, string impersonating, object[] args, ResultType shouldSucceed, StatusCode errorCode)
      tuple, out int userIndex, out ResultType shouldSucceed, out StatusCode errorCode)
  {
    switch (authType_)
    {
      case AuthenticationType.NoAuthentication:
        userIndex     = (int) tuple.userIndex;
        shouldSucceed = ResultType.AlwaysTrue;
        errorCode     = StatusCode.OK;
        break;
      case AuthenticationType.NoAuthorization:
        userIndex = (int) tuple.userIndex;
        shouldSucceed = tuple.shouldSucceed == ResultType.AlwaysTrue ? ResultType.AlwaysTrue : (tuple.errorCode == StatusCode.PermissionDenied || tuple.shouldSucceed == ResultType.AuthorizedForSome ? ResultType.AlwaysTrue : ResultType.AlwaysFalse);
        errorCode = tuple.errorCode == StatusCode.Unauthenticated
                      ? tuple.errorCode
                      : StatusCode.OK;
        break;
      default:
      case AuthenticationType.DefaultAuth:
        userIndex     = (int)tuple.userIndex;
        shouldSucceed = tuple.shouldSucceed;
        errorCode     = tuple.errorCode;
        break;
    }
  }

  [TestCaseSource(nameof(GetTestCases))]
  public async Task AuthMatchesBehavior(
    (string method, IdentityIndex userIndex, ImpersonationType impersonationType,string impersonating, object[] args, ResultType shouldSucceed, StatusCode errorCode) tuple)
  {
    TransformResult(tuple, out var userIndex, out var shouldSucceed, out var errorCode);
    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client = new SubmitterClient(channel);
    if (shouldSucceed == ResultType.AlwaysTrue || (shouldSucceed == ResultType.AuthorizedForSome && Identities[userIndex]
                                                                                                                .Permissions.Any(p => p.Name == tuple.method)))
    {
      Assert.DoesNotThrow(delegate
                          {
                            client.GetType()
                                  .InvokeMember(tuple.method,
                                                BindingFlags.InvokeMethod,
                                                null,
                                                client,
                                                tuple.args);
                          });
    }
    else
    {
      var exception = Assert.Catch(delegate
                                   {
                                     client.GetType()
                                           .InvokeMember(tuple.method,
                                                         BindingFlags.InvokeMethod,
                                                         null,
                                                         client,
                                                         tuple.args);
                                   });
      Assert.IsNotNull(exception);
      Assert.IsNotNull(exception.InnerException);
      Assert.IsInstanceOf<RpcException>(exception.InnerException);
      Assert.AreEqual(errorCode,
                      ((RpcException)(exception.InnerException)).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }
}
