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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Submitter;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;
using ArmoniK.Core.Common.Tests.Helpers;

using Google.Protobuf.WellKnownTypes;

using Grpc.Core;

using JetBrains.Annotations;

using NUnit.Framework;

using SubmitterClient = ArmoniK.Api.gRPC.V1.Submitter.Submitter.SubmitterClient;

namespace ArmoniK.Core.Common.Tests.Auth;

[TestFixture]
public class AuthenticationIntegrationTest
{
  public enum IdentityIndex
  {
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
    if (impersonate != null)
    {
      headers.Add(AuthenticatorOptions.Default.ImpersonationHeader!,
                  impersonate);
    }

    return headers;
  }

  public static object[] GetArgs(object             obj,
                                 IdentityIndex      identityIndex,
                                 [CanBeNull] string impersonnate)
  {
    return new[]
           {
             obj,
             GetHeaders(identityIndex,
                        impersonnate),
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

    // Parameters
    var parametersList = new List<object[]>
                     {
                       new object[]
                       {
                         IdentityIndex.AllRights,
                         ResultType.AlwaysTrue,
                         StatusCode.OK,
                         null,
                       },
                       new object[]
                       {
                         IdentityIndex.AllRights,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         "NoRightsUsername1",
                       },
                       new object[]
                       {
                         IdentityIndex.NoRights,
                         ResultType.AlwaysFalse,
                         StatusCode.PermissionDenied,
                         null,
                       },
                       new object[]
                       {
                         IdentityIndex.CanImpersonate,
                         ResultType.AlwaysTrue,
                         StatusCode.OK,
                         AllRightsUsername,
                       },
                       new object[]
                       {
                         IdentityIndex.CanImpersonate,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         "NoRightsUsername1",
                       },
                       new object[]
                       {
                         IdentityIndex.NoCertifcate,
                         ResultType.AlwaysFalse,
                         StatusCode.Unauthenticated,
                         null,
                       },
                       new object[]
                       {
                         IdentityIndex.SomeRights,
                         ResultType.AuthorizedForSome,
                         StatusCode.PermissionDenied,
                         null,
                       },
                     };

    // Generator
    foreach(var parameters in parametersList)
    {
      var identityIndex = (IdentityIndex)parameters[0];
      var    shouldSucceed = (ResultType)parameters[1];
      var    statusCode    = (StatusCode)parameters[2];
      var       impersonate   = (string)parameters[3];
      yield return (nameof(SubmitterClient.CreateSession), identityIndex, impersonate, GetArgs(new CreateSessionRequest
                                                                                             {
                                                                                               Id                = sessionId,
                                                                                               DefaultTaskOption = taskOptions,
                                                                                             },
                                                                                             identityIndex,
                                                                                             impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.CancelSession), identityIndex, impersonate, GetArgs(new Session
                                                                                             {
                                                                                               Id = sessionId,
                                                                                             },
                                                                                             identityIndex,
                                                                                             impersonate), shouldSucceed, statusCode);
      yield return (nameof(SubmitterClient.CountTasks), identityIndex, impersonate, GetArgs(new TaskFilter
                                                                                        {
                                                                                          Task = idsrequest,
                                                                                        },
                                                                                        identityIndex,
                                                                                        impersonate), shouldSucceed, statusCode);
    }
    
  }

  [OneTimeSetUp]
  public void BeforeAll()
  {
    var submitter = new SimpleSubmitter();
    helper_ = new GrpcSubmitterServiceHelper(submitter,
                                             Identities.ToList(),
                                             AuthenticatorOptions.Default);
  }

  [TestCaseSource(nameof(GetTestCases))]
  public async Task SubmitterServiceWithAuthShouldMatch(
    (string method, IdentityIndex userIndex, string impersonating, object[] args, ResultType shouldSucceed, StatusCode errorCode) tuple)
  {
    var channel = await helper_!.CreateChannel()
                                .ConfigureAwait(false);
    var client = new SubmitterClient(channel);
    if (tuple.shouldSucceed == ResultType.AlwaysTrue || (tuple.shouldSucceed == ResultType.AuthorizedForSome && Identities[(int)tuple.userIndex]
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
      Assert.AreEqual(tuple.errorCode,
                      ((RpcException)(exception.InnerException)).StatusCode);
    }

    await helper_.DeleteChannel()
                 .ConfigureAwait(false);
  }
}
