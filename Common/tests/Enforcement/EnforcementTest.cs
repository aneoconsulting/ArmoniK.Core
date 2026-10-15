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
using System.Reflection;
using System.Threading.Tasks;

using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Enforcement;

[TestFixture(TestOf = typeof(MethodAuthorization))]
public class MethodAuthorizationTest
{
  private static IEnumerable<(Type Service, MethodInfo Method)> Rpcs()
    => MethodAuthorization.Services.SelectMany(service => service.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                                                                 .Where(method => method.GetBaseDefinition()
                                                                                        .DeclaringType != method.DeclaringType)
                                                                 .Select(method => (service, method)));

  [Test]
  public void EveryRpcHasARule()
  {
    var missing = Rpcs()
                  .Where(rpc => !MethodAuthorization.Rules.ContainsKey((rpc.Service.Name, rpc.Method.Name)))
                  .Select(rpc => $"{rpc.Service.Name}.{rpc.Method.Name}")
                  .ToList();

    Assert.That(missing,
                Is.Empty);
  }

  [Test]
  public void EveryRuleNamesAnExistingRpc()
  {
    var rpcs = Rpcs()
               .Select(rpc => (rpc.Service.Name, rpc.Method.Name))
               .ToHashSet();

    Assert.That(MethodAuthorization.Rules.Keys.Where(key => !rpcs.Contains(key)),
                Is.Empty);
  }

  [TestCase(nameof(GrpcSubmitterService.CreateLargeTasks))]
  [TestCase(nameof(GrpcSubmitterService.CancelTasks))]
  [TestCase(nameof(GrpcSubmitterService.CountTasks))]
  [TestCase(nameof(GrpcSubmitterService.ListTasks))]
  [TestCase(nameof(GrpcSubmitterService.WaitForCompletion))]
  [TestCase(nameof(GrpcSubmitterService.ListSessions))]
  public void LegacyMethodsWithoutDecidableTargetsAreUnsupported(string method)
    => Assert.That(MethodAuthorization.For(nameof(GrpcSubmitterService),
                                           method)
                                      ?.Kind,
                   Is.EqualTo(RuleKind.Unsupported));

  [Test]
  public void AnRpcWithoutRuleIsUnsupported()
    => Assert.That(MethodAuthorization.For(nameof(GrpcSubmitterService),
                                           "NotAnRpc")
                                      ?.Kind,
                   Is.EqualTo(RuleKind.Unsupported));

  [Test]
  public void SessionCreationsSayWhichPartitionsTheyDeclare()
    => Assert.That(MethodAuthorization.Rules.Where(pair => pair.Value.Kind == RuleKind.SessionCreation && pair.Value.Creation is null)
                                      .Select(pair => pair.Key),
                   Is.Empty);

  [Test]
  public void AServiceOutsideTheApiHasNoRule()
    => Assert.That(MethodAuthorization.For("ReflectionServiceImpl",
                                           "ServerReflectionInfo"),
                   Is.Null);

  [Test]
  public void AnUnknownServiceIsUnsupported()
    => Assert.That(MethodAuthorization.For("GrpcFooService",
                                           "Foo")
                                      ?.Kind,
                   Is.EqualTo(RuleKind.Unsupported));

  [Test]
  public void RulesDecidedOnAResourceSayHowToFindIt()
    => Assert.That(MethodAuthorization.Rules.Where(pair => pair.Value.Kind is RuleKind.Session or RuleKind.Partition && pair.Value.Extract is null)
                                      .Select(pair => pair.Key),
                   Is.Empty);

  [Test]
  public void RulesRequiringAPermissionRequireOneOfTheCatalogue()
    => Assert.That(MethodAuthorization.Rules.Values.Where(rule => rule.Kind is RuleKind.Session or RuleKind.Partition or RuleKind.Listing or RuleKind.SessionCreation)
                                      .Where(rule => rule.Permissions.Count == 0 || rule.Permissions.Any(permission => !Permission.All.Contains(permission))),
                   Is.Empty);

  [Test]
  public void SubmissionIsDecidedOnTheSessionAndChecksTheResultsItUses()
  {
    var rule = MethodAuthorization.Rules[(nameof(GrpcTasksService), nameof(GrpcTasksService.SubmitTasks))];
    var targets = rule.Extract!(new Api.gRPC.V1.Tasks.SubmitTasksRequest
                                {
                                  SessionId = "s",
                                  TaskCreations =
                                  {
                                    new Api.gRPC.V1.Tasks.SubmitTasksRequest.Types.TaskCreation
                                    {
                                      PayloadId = "payload",
                                      DataDependencies =
                                      {
                                        "dependency",
                                      },
                                      ExpectedOutputKeys =
                                      {
                                        "output",
                                      },
                                    },
                                  },
                                });

    Assert.That(rule.Kind,
                Is.EqualTo(RuleKind.Session));
    Assert.That(rule.Permissions,
                Is.EqualTo(new[]
                           {
                             Permission.TaskSubmit,
                           }));
    Assert.That(targets.SessionId,
                Is.EqualTo("s"));
    Assert.That(targets.ReferencedResultIds,
                Is.EquivalentTo(new[]
                                {
                                  "payload",
                                  "dependency",
                                  "output",
                                }));
  }

  [Test]
  public void CreatingResultsWithTheirDataRequiresDeclaringAndProviding()
    => Assert.That(MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.CreateResults))]
                                      .Permissions,
                   Is.EquivalentTo(new[]
                                   {
                                     Permission.DataDeclare,
                                     Permission.DataProvide,
                                   }));

  [Test]
  public void AnUploadStartingWithDataIsAnInvalidArgument()
  {
    var exception = Assert.Throws<RpcException>(() => MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.UploadResultData))]
                                                                         .Extract!(new Api.gRPC.V1.Results.UploadResultDataRequest
                                                                                   {
                                                                                     DataChunk = ByteString.CopyFromUtf8("data"),
                                                                                   }));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.InvalidArgument));
  }

  [Test]
  public void ATaskOutputRequestWithoutSessionIsDecidedOnTheTask()
  {
    var targets = MethodAuthorization.Rules[(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.TryGetTaskOutput))]
                                     .Extract!(new Api.gRPC.V1.TaskOutputRequest
                                               {
                                                 TaskId = "task",
                                               });

    Assert.That(targets.SessionId,
                Is.Null);
    Assert.That(targets.TaskIds,
                Is.EqualTo(new[]
                           {
                             "task",
                           }));
  }

  private static Api.gRPC.V1.Tasks.FilterField SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField field,
                                                            string                                 value,
                                                            Api.gRPC.V1.FilterStringOperator       op = Api.gRPC.V1.FilterStringOperator.Equal)
    => new()
       {
         Field = new Api.gRPC.V1.Tasks.TaskField
                 {
                   TaskSummaryField = new Api.gRPC.V1.Tasks.TaskSummaryField
                                      {
                                        Field = field,
                                      },
                 },
         FilterString = new Api.gRPC.V1.FilterString
                        {
                          Value    = value,
                          Operator = op,
                        },
       };

  private static IReadOnlyCollection<string>? ListedTasks(params Api.gRPC.V1.Tasks.FilterField[][] branches)
  {
    var filters = new Api.gRPC.V1.Tasks.Filters();
    foreach (var branch in branches)
    {
      var and = new Api.gRPC.V1.Tasks.FiltersAnd();
      and.And.AddRange(branch);
      filters.Or.Add(and);
    }

    return MethodAuthorization.Rules[(nameof(GrpcTasksService), nameof(GrpcTasksService.ListTasks))]
                              .ListedSessions!(new Api.gRPC.V1.Tasks.ListTasksRequest
                                               {
                                                 Filters = filters,
                                               });
  }

  [Test]
  public void AListingWithoutFilterReachesAnySession()
    => Assert.That(ListedTasks(),
                   Is.Null);

  [Test]
  public void AListingRequiringASessionIsConfinedToIt()
    => Assert.That(ListedTasks([SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.TaskId, "t1"), SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.SessionId, "s1")]),
                   Is.EquivalentTo(new[]
                                   {
                                     "s1",
                                   }));

  [Test]
  public void AListingIsConfinedToTheSessionsOfAllItsBranches()
    => Assert.That(ListedTasks([SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.SessionId, "s1")],
                               [SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.SessionId, "s2")]),
                   Is.EquivalentTo(new[]
                                   {
                                     "s1",
                                     "s2",
                                   }));

  [Test]
  public void ABranchWithoutSessionReachesAnySession()
    => Assert.That(ListedTasks([SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.SessionId, "s1")],
                               [SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.TaskId, "t1")]),
                   Is.Null);

  [Test]
  public void ASessionOtherThanEqualReachesAnySession()
    => Assert.That(ListedTasks([SummaryField(Api.gRPC.V1.Tasks.TaskSummaryEnumField.SessionId, "s1", Api.gRPC.V1.FilterStringOperator.NotEqual)]),
                   Is.Null);

  [Test]
  public void AConditionWithoutFieldReachesAnySession()
    => Assert.That(ListedTasks([
                     new Api.gRPC.V1.Tasks.FilterField
                     {
                       FilterString = new Api.gRPC.V1.FilterString
                                      {
                                        Value    = "s1",
                                        Operator = Api.gRPC.V1.FilterStringOperator.Equal,
                                      },
                     },
                   ]),
                   Is.Null);

  [Test]
  public void AResultConditionWithoutFieldReachesAnySession()
  {
    var filters = new Api.gRPC.V1.Results.Filters
                  {
                    Or =
                    {
                      new Api.gRPC.V1.Results.FiltersAnd
                      {
                        And =
                        {
                          new Api.gRPC.V1.Results.FilterField
                          {
                            FilterString = new Api.gRPC.V1.FilterString
                                           {
                                             Value    = "s1",
                                             Operator = Api.gRPC.V1.FilterStringOperator.Equal,
                                           },
                          },
                        },
                      },
                    },
                  };

    Assert.That(MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.ListResults))]
                                   .ListedSessions!(new Api.gRPC.V1.Results.ListResultsRequest
                                                    {
                                                      Filters = filters,
                                                    }),
                Is.Null);
  }

  [Test]
  public void AResultListingRequiringASessionIsConfinedToIt()
  {
    var filters = new Api.gRPC.V1.Results.Filters
                  {
                    Or =
                    {
                      new Api.gRPC.V1.Results.FiltersAnd
                      {
                        And =
                        {
                          new Api.gRPC.V1.Results.FilterField
                          {
                            Field = new Api.gRPC.V1.Results.ResultField
                                    {
                                      ResultRawField = new Api.gRPC.V1.Results.ResultRawField
                                                       {
                                                         Field = Api.gRPC.V1.Results.ResultRawEnumField.SessionId,
                                                       },
                                    },
                            FilterString = new Api.gRPC.V1.FilterString
                                           {
                                             Value    = "s1",
                                             Operator = Api.gRPC.V1.FilterStringOperator.Equal,
                                           },
                          },
                        },
                      },
                    },
                  };

    Assert.That(MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.ListResults))]
                                   .ListedSessions!(new Api.gRPC.V1.Results.ListResultsRequest
                                                    {
                                                      Filters = filters,
                                                    }),
                Is.EquivalentTo(new[]
                                {
                                  "s1",
                                }));
  }
}

[TestFixture(TestOf = typeof(AuthorizationConfiguration))]
public class AuthorizationConfigurationTest
{
  [Test]
  public void GrantIdentifiersAreUnique()
    => Assert.That(() => AuthorizationConfiguration.Grants(new[]
                                                           {
                                                             new GrantOption
                                                             {
                                                               Id            = "g",
                                                               Principal     = "role:a",
                                                               PermissionSet = "session-view",
                                                             },
                                                             new GrantOption
                                                             {
                                                               Id            = "g",
                                                               Principal     = "role:b",
                                                               PermissionSet = "session-view",
                                                             },
                                                           }),
                   Throws.InstanceOf<InvalidOperationException>());

  [Test]
  public void ConfiguredSetsRedefineShippedOnes()
  {
    var sets = AuthorizationConfiguration.PermissionSets(new[]
                                                               {
                                                                 new PermissionSetOption
                                                                 {
                                                                   Name = "session-view",
                                                                   Permissions =
                                                                   {
                                                                     "session:read",
                                                                   },
                                                                 },
                                                                 new PermissionSetOption
                                                                 {
                                                                   Name = "team-reader",
                                                                   Permissions =
                                                                   {
                                                                     "data:read",
                                                                     "data:download",
                                                                   },
                                                                 },
                                                               });

    Assert.That(sets["session-view"]
                  .Permissions,
                Is.EquivalentTo(new[]
                                {
                                  Permission.SessionRead,
                                }));
    Assert.That(sets["team-reader"]
                  .Permissions,
                Has.Count.EqualTo(2));
    Assert.That(sets["session-own"],
                Is.SameAs(ShippedPermissionSets.SessionOwn));
  }

  [TestCase("cluster-admin",
            "session:read")]
  [TestCase("custom",
            "session:control")]
  [TestCase("",
            "session:read")]
  public void InvalidSetsPreventStarting(string name,
                                         string permission)
    => Assert.That(() => AuthorizationConfiguration.PermissionSets(new[]
                                                                         {
                                                                           new PermissionSetOption
                                                                           {
                                                                             Name = name,
                                                                             Permissions =
                                                                             {
                                                                               permission,
                                                                             },
                                                                           },
                                                                         }),
                   Throws.InstanceOf<InvalidOperationException>());
}

[TestFixture(TestOf = typeof(CallerResolver))]
public class CallerResolverTest
{
  private static readonly CallerResolver Resolver = new(new IdentityOptions());

  private static HttpContext Request(params (string Name, string Value)[] headers)
  {
    var context = new DefaultHttpContext();
    foreach (var (name, value) in headers)
    {
      context.Request.Headers[name] = value;
    }

    return context;
  }

  private static string Certificate(string subject)
  {
    using var key = System.Security.Cryptography.RSA.Create(2048);
    using var certificate = new System.Security.Cryptography.X509Certificates.CertificateRequest(subject,
                                                                                               key,
                                                                                               System.Security.Cryptography.HashAlgorithmName.SHA256,
                                                                                               System.Security.Cryptography.RSASignaturePadding.Pkcs1)
                              .CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                                DateTimeOffset.UtcNow.AddDays(1));
    return Uri.EscapeDataString(certificate.ExportCertificatePem());
  }

  [Test]
  public async Task AForwardedCertificateNamesTheCaller()
  {
    var caller = await Resolver.ResolveAsync(Request(("X-Forwarded-Client-Cert", Certificate("CN=client, OU=supervisors"))))
                               .ConfigureAwait(false);

    Assert.That(caller!.Caller.Expansion,
                Is.EqualTo(new[]
                           {
                             "cn:client",
                             "role:supervisors",
                             "*",
                           }));
    Assert.That(caller.CredentialKind,
                Is.EqualTo(CredentialKind.Certificate));
  }

  // Unsigned, or signed with a key the control plane does not know, and without issuer, audience nor lifetime
  [TestCase("none",
            "")]
  [TestCase("RS256",
            "c2lnbmF0dXJl")]
  public async Task AnUnvalidatedTokenNamesTheCallerFromItsClaims(string algorithm,
                                                                  string signature)
  {
    var provider = new ServiceCollection().AddLogging();
    provider.AddArmoniKAuthorization(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                                                                                      {
                                                                                        ["Authorization:Enabled"]   = "true",
                                                                                        ["Identity:ValidateTokens"] = "false",
                                                                                      })
                                                               .Build());
    var services = provider.BuildServiceProvider();

    var token = $"{Base64UrlEncoder.Encode($$"""{"alg":"{{algorithm}}","typ":"JWT"}""")}.{Base64UrlEncoder.Encode("""{"oid":"alice","roles":["team"]}""")}.{signature}";
    var request = Request(("Authorization", $"Bearer {token}"));
    request.RequestServices = services;

    var caller = await services.GetRequiredService<CallerResolver>()
                               .ResolveAsync(request)
                               .ConfigureAwait(false);

    Assert.That(caller!.Caller.Expansion,
                Is.EqualTo(new[]
                           {
                             "oid:alice",
                             "role:team",
                             "*",
                           }));
    Assert.That(caller.CredentialKind,
                Is.EqualTo(CredentialKind.Token));
  }

  [Test]
  public async Task NoCredentialIsNoCaller()
    => Assert.That(await Resolver.ResolveAsync(Request())
                                 .ConfigureAwait(false),
                   Is.Null);

  [Test]
  public void ATokenAndACertificateTogetherAreRefused()
    => Assert.That(() => Resolver.ResolveAsync(Request(("Authorization", "Bearer x"),
                                                       ("X-Forwarded-Client-Cert", Certificate("CN=client")))),
                   Throws.InstanceOf<FormatException>());

  [Test]
  public void ATokenIsRefusedWhenTokensAreNotValidated()
    => Assert.That(() => Resolver.ResolveAsync(Request(("Authorization", "Bearer x"))),
                   Throws.InstanceOf<FormatException>());

  [Test]
  public void AGarbledCertificateIsRefused()
    => Assert.That(() => Resolver.ResolveAsync(Request(("X-Forwarded-Client-Cert", "not-a-certificate"))),
                   Throws.InstanceOf<FormatException>());
}

[TestFixture(TestOf = typeof(Common.Enforcement.ServiceCollectionExt))]
public class AddArmoniKAuthorizationTest
{
  private static IConfiguration Configuration(params (string Key, string Value)[] values)
    => new ConfigurationBuilder().AddInMemoryCollection(values.Select(value => new KeyValuePair<string, string?>(value.Key,
                                                                                                                 value.Value)))
                                 .Build();

  [Test]
  public void DisabledAuthorizationRegistersNothing()
  {
    var services = new ServiceCollection();

    Assert.That(services.AddArmoniKAuthorization(Configuration()),
                Is.False);
    Assert.That(services,
                Is.Empty);
  }

  [Test]
  public void EnabledAuthorizationRegistersTheInterceptor()
  {
    var services = new ServiceCollection();

    Assert.That(services.AddArmoniKAuthorization(Configuration(("Authorization:Enabled", "true"),
                                                               ("Authorization:Grants:0:Id", "admin"),
                                                               ("Authorization:Grants:0:Principal", "role:platform"),
                                                               ("Authorization:Grants:0:PermissionSet", "cluster-admin"))),
                Is.True);
    Assert.That(services.Any(service => service.ServiceType == typeof(AuthorizationInterceptor)),
                Is.True);
  }

  [TestCase("session.owner = request.caller")]
  [TestCase("")]
  public void ARefusedGrantPreventsStarting(string condition)
    => Assert.That(() => new ServiceCollection().AddArmoniKAuthorization(Configuration(("Authorization:Enabled", "true"),
                                                                                       ("Authorization:Grants:0:Id", "broken"),
                                                                                       ("Authorization:Grants:0:Principal", "role:team"),
                                                                                       ("Authorization:Grants:0:PermissionSet", "session-own"),
                                                                                       ("Authorization:Grants:0:Condition", condition))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("broken"));

  [TestCase("Authenticator:RequireAuthentication",
            "false")]
  [TestCase("InitServices:Authentication:Users:0",
            "{}")]
  [TestCase("Components:AuthenticationStorage",
            "ArmoniK.Adapters.MongoDB.AuthenticationTable")]
  public void RemovedAuthenticationSettingsRequireAnExplicitChoice(string key,
                                                                   string value)
  {
    Assert.That(() => new ServiceCollection().AddArmoniKAuthorization(Configuration((key, value))),
                Throws.InstanceOf<InvalidOperationException>()
                      .With.Message.Contains(key.Split(':')[0]));
    Assert.That(new ServiceCollection().AddArmoniKAuthorization(Configuration((key, value),
                                                                              ("Authorization:Enabled", "false"))),
                Is.False);
  }

  [Test]
  public void AnAuthorityRequiresAnAudience()
    => Assert.That(() => new ServiceCollection().AddArmoniKAuthorization(Configuration(("Authorization:Enabled", "true"),
                                                                                       ("Identity:Authority", "https://idp.example"))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("Audience"));

  [Test]
  public void AnAuthorityIsRefusedWhenTokensAreNotValidated()
    => Assert.That(() => new ServiceCollection().AddArmoniKAuthorization(Configuration(("Authorization:Enabled", "true"),
                                                                                       ("Identity:Authority", "https://idp.example"),
                                                                                       ("Identity:Audience", "armonik"),
                                                                                       ("Identity:ValidateTokens", "false"))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("ValidateTokens"));
}
