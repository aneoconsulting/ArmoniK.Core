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
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Core.Authorization;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.gRPC.Services;
using ArmoniK.Core.Common.Injection.Options;

using Google.Protobuf;

using Grpc.Core;

using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

using Moq;

using NUnit.Framework;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;

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
    var targets = rule.Extract!(new SubmitTasksRequest
                                {
                                  SessionId = "s",
                                  TaskCreations =
                                  {
                                    new SubmitTasksRequest.Types.TaskCreation
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
    => Assert.That(MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.CreateResults))].Permissions,
                   Is.EquivalentTo(new[]
                                   {
                                     Permission.DataDeclare,
                                     Permission.DataProvide,
                                   }));

  [Test]
  public void CancellingTasksRequiresReadingThem()
    => Assert.That(MethodAuthorization.Rules[(nameof(GrpcTasksService), nameof(GrpcTasksService.CancelTasks))].Permissions,
                   Is.EquivalentTo(new[]
                                   {
                                     Permission.TaskCancel,
                                     Permission.TaskRead,
                                   }));

  [Test]
  public void DownloadingAResultWithTheLegacySubmitterRequiresReadingItsTask()
    => Assert.That(MethodAuthorization.Rules[(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.TryGetResultStream))].Permissions,
                   Is.EquivalentTo(new[]
                                   {
                                     Permission.DataDownload,
                                     Permission.TaskRead,
                                   }));

  [Test]
  public void AnUploadStartingWithDataIsAnInvalidArgument()
  {
    var exception =
      Assert.Throws<RpcException>(() => MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.UploadResultData))]
                                                           .Extract!(new UploadResultDataRequest
                                                                     {
                                                                       DataChunk = ByteString.CopyFromUtf8("data"),
                                                                     }));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.InvalidArgument));
  }

  [Test]
  public void ATaskOutputRequestWithoutSessionIsDecidedOnTheTask()
  {
    var targets = MethodAuthorization.Rules[(nameof(GrpcSubmitterService), nameof(GrpcSubmitterService.TryGetTaskOutput))].Extract!(new TaskOutputRequest
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

  private static FilterField SummaryField(TaskSummaryEnumField field,
                                          string               value,
                                          FilterStringOperator op = FilterStringOperator.Equal)
    => new()
       {
         Field = new TaskField
                 {
                   TaskSummaryField = new TaskSummaryField
                                      {
                                        Field = field,
                                      },
                 },
         FilterString = new FilterString
                        {
                          Value    = value,
                          Operator = op,
                        },
       };

  private static IReadOnlyCollection<string>? ListedTasks(params FilterField[][] branches)
  {
    var filters = new Filters();
    foreach (var branch in branches)
    {
      var and = new FiltersAnd();
      and.And.AddRange(branch);
      filters.Or.Add(and);
    }

    return MethodAuthorization.Rules[(nameof(GrpcTasksService), nameof(GrpcTasksService.ListTasks))].ListedSessions!(new ListTasksRequest
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
    => Assert.That(ListedTasks([
                     SummaryField(TaskSummaryEnumField.TaskId,
                                  "t1"),
                     SummaryField(TaskSummaryEnumField.SessionId,
                                  "s1"),
                   ]),
                   Is.EquivalentTo(new[]
                                   {
                                     "s1",
                                   }));

  [Test]
  public void AListingIsConfinedToTheSessionsOfAllItsBranches()
    => Assert.That(ListedTasks([
                                 SummaryField(TaskSummaryEnumField.SessionId,
                                              "s1"),
                               ],
                               [
                                 SummaryField(TaskSummaryEnumField.SessionId,
                                              "s2"),
                               ]),
                   Is.EquivalentTo(new[]
                                   {
                                     "s1",
                                     "s2",
                                   }));

  [Test]
  public void ABranchWithoutSessionReachesAnySession()
    => Assert.That(ListedTasks([
                                 SummaryField(TaskSummaryEnumField.SessionId,
                                              "s1"),
                               ],
                               [
                                 SummaryField(TaskSummaryEnumField.TaskId,
                                              "t1"),
                               ]),
                   Is.Null);

  [Test]
  public void ASessionOtherThanEqualReachesAnySession()
    => Assert.That(ListedTasks([
                     SummaryField(TaskSummaryEnumField.SessionId,
                                  "s1",
                                  FilterStringOperator.NotEqual),
                   ]),
                   Is.Null);

  [Test]
  public void AConditionWithoutFieldReachesAnySession()
    => Assert.That(ListedTasks([
                     new FilterField
                     {
                       FilterString = new FilterString
                                      {
                                        Value    = "s1",
                                        Operator = FilterStringOperator.Equal,
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
                            FilterString = new FilterString
                                           {
                                             Value    = "s1",
                                             Operator = FilterStringOperator.Equal,
                                           },
                          },
                        },
                      },
                    },
                  };

    Assert.That(MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.ListResults))].ListedSessions!(new ListResultsRequest
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
                            Field = new ResultField
                                    {
                                      ResultRawField = new ResultRawField
                                                       {
                                                         Field = ResultRawEnumField.SessionId,
                                                       },
                                    },
                            FilterString = new FilterString
                                           {
                                             Value    = "s1",
                                             Operator = FilterStringOperator.Equal,
                                           },
                          },
                        },
                      },
                    },
                  };

    Assert.That(MethodAuthorization.Rules[(nameof(GrpcResultsService), nameof(GrpcResultsService.ListResults))].ListedSessions!(new ListResultsRequest
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
  public void OnlyTheConfiguredSetsExist()
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
                                                             Name = "cluster-admin",
                                                             Permissions =
                                                             {
                                                               "*",
                                                             },
                                                           },
                                                         });

    Assert.That(sets.Keys,
                Is.EquivalentTo(new[]
                                {
                                  "session-view",
                                  "cluster-admin",
                                }));
    Assert.That(sets["session-view"].Permissions,
                Is.EquivalentTo(new[]
                                {
                                  Permission.SessionRead,
                                }));
    Assert.That(sets["cluster-admin"].Permissions,
                Is.EquivalentTo(Permission.All));
  }

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
  private static readonly CallerResolver Resolver = new(new AuthorizationOptions
                                                        {
                                                          Authentication = CallerAuthentication.Xfcc | CallerAuthentication.Token,
                                                        },
                                                        new IdentityOptions());

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
    using var key = RSA.Create(2048);
    using var certificate = new CertificateRequest(subject,
                                                   key,
                                                   HashAlgorithmName.SHA256,
                                                   RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                                                                               DateTimeOffset.UtcNow.AddDays(1));
    return $"Cert=\"{Uri.EscapeDataString(certificate.ExportCertificatePem())}\"";
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

  /// <summary>
  ///   Services taking tokens without validating them, with the given methods
  /// </summary>
  private static IServiceProvider Unvalidated(string authentication)
  {
    var services = new ServiceCollection().AddLogging();
    services.AddArmoniKAuthorization(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                                                                                      {
                                                                                        ["Authorization:Authentication"] = authentication,
                                                                                        ["Identity:ValidateTokens"]      = "false",
                                                                                      })
                                                               .Build(),
                                     NullLogger.Instance);
    return services.BuildServiceProvider();
  }

  private static string UnvalidatedToken(string algorithm = "none",
                                         string signature = "")
    => $"{Base64UrlEncoder.Encode($$"""{"alg":"{{algorithm}}","typ":"JWT"}""")}.{Base64UrlEncoder.Encode("""{"oid":"alice","roles":["team"]}""")}.{signature}";

  // A proxy in front of the control plane may add its own certificate header to the requests of token callers
  [Test]
  public async Task ACertificateIsIgnoredWhenOnlyTokensAreSelected()
  {
    var services = Unvalidated("Token");
    var resolver = services.GetRequiredService<CallerResolver>();

    var both = Request(("Authorization", $"Bearer {UnvalidatedToken()}"),
                       ("X-Forwarded-Client-Cert", Certificate("CN=proxy")));
    both.RequestServices = services;
    var certificateOnly = Request(("X-Forwarded-Client-Cert", Certificate("CN=client")));
    certificateOnly.RequestServices = services;

    Assert.That((await resolver.ResolveAsync(both)
                               .ConfigureAwait(false))!.Caller.Identity.Value,
                Is.EqualTo("oid:alice"));
    Assert.That(await resolver.ResolveAsync(certificateOnly)
                              .ConfigureAwait(false),
                Is.Null);
  }

  [Test]
  public async Task ATokenIsIgnoredWhenOnlyCertificatesAreSelected()
  {
    var services = new ServiceCollection().AddLogging();
    services.AddArmoniKAuthorization(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                                                                                      {
                                                                                        ["Authorization:Authentication"] = "Xfcc",
                                                                                      })
                                                               .Build(),
                                     NullLogger.Instance);
    var resolver = services.BuildServiceProvider()
                           .GetRequiredService<CallerResolver>();

    Assert.That((await resolver.ResolveAsync(Request(("Authorization", $"Bearer {UnvalidatedToken()}"),
                                                     ("X-Forwarded-Client-Cert", Certificate("CN=client"))))
                               .ConfigureAwait(false))!.Caller.Identity.Value,
                Is.EqualTo("cn:client"));
    Assert.That(await resolver.ResolveAsync(Request(("Authorization", $"Bearer {UnvalidatedToken()}")))
                              .ConfigureAwait(false),
                Is.Null);
  }

  // Unsigned, or signed with a key the control plane does not know, and without issuer, audience nor lifetime
  [TestCase("none",
            "")]
  [TestCase("RS256",
            "c2lnbmF0dXJl")]
  public async Task AnUnvalidatedTokenNamesTheCallerFromItsClaims(string algorithm,
                                                                  string signature)
  {
    var services = Unvalidated("Token");

    var request = Request(("Authorization", $"Bearer {UnvalidatedToken(algorithm, signature)}"));
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

  private const string Issuer = "https://authority.test";

  /// <summary>
  ///   Services validating tokens against an authority, its metadata read from <paramref name="metadata" /> when
  ///   given
  /// </summary>
  private static IServiceProvider Validating(OpenIdConnectConfiguration? metadata)
  {
    var services = new ServiceCollection().AddLogging();
    services.AddArmoniKAuthorization(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                                                                                      {
                                                                                        ["Authorization:Authentication"] = "Token",
                                                                                        // Nothing listens there: the metadata cannot be read
                                                                                        ["Identity:Authority"] = "https://127.0.0.1:1/realm",
                                                                                        ["Identity:Audience"]  = "armonik",
                                                                                      })
                                                               .Build(),
                                     NullLogger.Instance);
    if (metadata is not null)
    {
      services.PostConfigure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme,
                                               jwt => jwt.ConfigurationManager = new StaticConfigurationManager<OpenIdConnectConfiguration>(metadata));
    }

    return services.BuildServiceProvider();
  }

  private static OpenIdConnectConfiguration Metadata(SecurityKey key)
  {
    var metadata = new OpenIdConnectConfiguration
                   {
                     Issuer = Issuer,
                   };
    metadata.SigningKeys.Add(key);
    return metadata;
  }

  private static string Token(SecurityKey key)
    => new JsonWebTokenHandler().CreateToken(new SecurityTokenDescriptor
                                             {
                                               Issuer   = Issuer,
                                               Audience = "armonik",
                                               Expires  = DateTime.UtcNow.AddMinutes(5),
                                               Claims = new Dictionary<string, object>
                                                        {
                                                          ["oid"] = "alice",
                                                        },
                                               SigningCredentials = new SigningCredentials(key,
                                                                                           SecurityAlgorithms.RsaSha256),
                                             });

  private static Task<AuthenticatedCaller?> ResolveToken(IServiceProvider services,
                                                         string           token)
  {
    var request = Request(("Authorization", $"Bearer {token}"));
    request.RequestServices = services;
    return services.GetRequiredService<CallerResolver>()
                   .ResolveAsync(request);
  }

  [Test]
  public void ATokenIsNotAnsweredAsInvalidWhileTheMetadataOfTheAuthorityCannotBeRead()
  {
    // Any exception but a FormatException is answered as unavailable
    var exception = Assert.CatchAsync(() => ResolveToken(Validating(null),
                                                         Token(new RsaSecurityKey(RSA.Create(2048)))));

    Assert.That(exception,
                Is.Not.InstanceOf<FormatException>());
  }

  [Test]
  public async Task ATokenSignedByTheAuthorityNamesTheCaller()
  {
    var key = new RsaSecurityKey(RSA.Create(2048));

    var caller = await ResolveToken(Validating(Metadata(key)),
                                    Token(key))
                   .ConfigureAwait(false);

    Assert.That(caller!.Caller.Identity.Value,
                Is.EqualTo("oid:alice"));
  }

  [Test]
  public void ATokenNotSignedByTheAuthorityIsInvalid()
    => Assert.That(() => ResolveToken(Validating(Metadata(new RsaSecurityKey(RSA.Create(2048)))),
                                      Token(new RsaSecurityKey(RSA.Create(2048)))),
                   Throws.InstanceOf<FormatException>());

  [Test]
  public async Task TheControlPlaneIsReadyOnceTheMetadataOfTheAuthorityIsRead()
  {
    var unreachable = Validating(null)
      .GetRequiredService<CallerResolver>();
    var reachable = Validating(Metadata(new RsaSecurityKey(RSA.Create(2048))))
      .GetRequiredService<CallerResolver>();

    Assert.That((await unreachable.Check(HealthCheckTag.Readiness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Unhealthy));
    Assert.That((await unreachable.Check(HealthCheckTag.Liveness)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    Assert.That((await unreachable.Check(HealthCheckTag.Startup)
                                  .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
    Assert.That((await reachable.Check(HealthCheckTag.Readiness)
                                .ConfigureAwait(false)).Status,
                Is.EqualTo(HealthStatus.Healthy));
  }

  [Test]
  public void TheMetadataIsReadAgainSoonAfterAFailure()
    => Assert.That(Validating(null)
                   .GetRequiredService<IOptionsMonitor<JwtBearerOptions>>()
                   .Get(JwtBearerDefaults.AuthenticationScheme)
                   .RefreshInterval,
                   Is.EqualTo(TimeSpan.FromSeconds(30)));

  [Test]
  public async Task NoCredentialIsNoCaller()
    => Assert.That(await Resolver.ResolveAsync(Request())
                                 .ConfigureAwait(false),
                   Is.Null);

  [Test]
  public void ATokenAndACertificateTogetherAreRefusedWhenBothAreSelected()
    => Assert.That(() => Resolver.ResolveAsync(Request(("Authorization", "Bearer x"),
                                                       ("X-Forwarded-Client-Cert", Certificate("CN=client")))),
                   Throws.InstanceOf<FormatException>());

  [Test]
  public void AGarbledCertificateIsRefused()
    => Assert.That(() => Resolver.ResolveAsync(Request(("X-Forwarded-Client-Cert", "not-a-certificate"))),
                   Throws.InstanceOf<FormatException>());
}

[TestFixture(TestOf = typeof(ServiceCollectionExt))]
public class AddArmoniKAuthorizationTest
{
  private static IConfiguration Configuration(params (string Key, string Value)[] values)
    => new ConfigurationBuilder().AddInMemoryCollection(values.Select(value => new KeyValuePair<string, string?>(value.Key,
                                                                                                                 value.Value)))
                                 .Build();

  private static bool Add(IServiceCollection services,
                          IConfiguration     configuration,
                          ILogger?           logger = null)
    => services.AddArmoniKAuthorization(configuration,
                                        logger ?? NullLogger.Instance);

  private static bool Add(IConfiguration configuration)
    => Add(new ServiceCollection(),
           configuration);

  private static bool RegistersTokens(IServiceCollection services)
    => services.Any(service => service.ServiceType == typeof(IAuthenticationService));

  [Test]
  public void DisabledAuthorizationRegistersNothing()
  {
    var services = new ServiceCollection();

    Assert.That(Add(services,
                    Configuration()),
                Is.False);
    Assert.That(services,
                Is.Empty);
  }

  // The settings of the methods are ignored, so that authorization can always be disabled
  [Test]
  public void NoMethodRegistersNothingWhateverTheIdentitySettings()
  {
    var services = new ServiceCollection();

    Assert.That(Add(services,
                    Configuration(("Authorization:Authentication", "None"),
                                  ("Identity:Authority", "not a uri"),
                                  ("Identity:CertificateHeader", ""))),
                Is.False);
    Assert.That(services,
                Is.Empty);
  }

  [Test]
  public void CertificatesRegisterTheInterceptorWithoutTokens()
  {
    var services = new ServiceCollection();

    Assert.That(Add(services,
                    Configuration(("Authorization:Authentication", "Xfcc"),
                                  ("Authorization:Grants:0:Id", "admin"),
                                  ("Authorization:Grants:0:Principal", "role:platform"),
                                  ("Authorization:Grants:0:PermissionSet", "cluster-admin"),
                                  ("Authorization:PermissionSets:0:Name", "cluster-admin"),
                                  ("Authorization:PermissionSets:0:Permissions:0", "*"))),
                Is.True);
    Assert.That(services.Any(service => service.ServiceType == typeof(AuthorizationInterceptor)),
                Is.True);
    Assert.That(RegistersTokens(services),
                Is.False);
  }

  [Test]
  public void TokensRegisterTheirValidation()
  {
    var services = new ServiceCollection();

    Assert.That(Add(services,
                    Configuration(("Authorization:Authentication", "Token"),
                                  ("Identity:Authority", "https://idp.example"),
                                  ("Identity:Audience", "armonik"))),
                Is.True);
    Assert.That(services.Any(service => service.ServiceType == typeof(AuthorizationInterceptor)),
                Is.True);
    Assert.That(RegistersTokens(services),
                Is.True);
  }

  // Environment variables are read as Authorization__Authentication=Xfcc,Token
  [TestCase("Xfcc,Token")]
  [TestCase("Xfcc, Token")]
  [TestCase("Token,Xfcc")]
  public void BothMethodsAreReadFromTheEnvironment(string value)
  {
    const string prefix = "ARMONIK_AUTHENTICATION_TEST_";
    Environment.SetEnvironmentVariable($"{prefix}Authorization__Authentication",
                                       value);
    Environment.SetEnvironmentVariable($"{prefix}Identity__ValidateTokens",
                                       "false");
    try
    {
      var configuration = new ConfigurationBuilder().AddEnvironmentVariables(prefix)
                                                    .Build();
      var services = new ServiceCollection();

      Assert.That(Add(services,
                      configuration),
                  Is.True);
      Assert.That(RegistersTokens(services),
                  Is.True);
      // The certificate header is checked: certificates are selected too
      Environment.SetEnvironmentVariable($"{prefix}Identity__CertificateHeader",
                                         " ");
      Assert.That(() => Add(new ConfigurationBuilder().AddEnvironmentVariables(prefix)
                                                      .Build()),
                  Throws.InstanceOf<InvalidOperationException>()
                        .With.Message.Contains("CertificateHeader"));
    }
    finally
    {
      Environment.SetEnvironmentVariable($"{prefix}Authorization__Authentication",
                                         null);
      Environment.SetEnvironmentVariable($"{prefix}Identity__ValidateTokens",
                                         null);
      Environment.SetEnvironmentVariable($"{prefix}Identity__CertificateHeader",
                                         null);
    }
  }

  [TestCase("4")]
  [TestCase("7")]
  [TestCase("-1")]
  [TestCase("Mtls")]
  [TestCase("Xfcc,Mtls")]
  public void AnUnknownMethodPreventsStarting(string value)
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", value),
                                           ("Identity:ValidateTokens", "false"))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("Authorization:Authentication"));

  [TestCase("true")]
  [TestCase("false")]
  public void TheRemovedEnabledSettingPreventsStarting(string value)
    => Assert.That(() => Add(Configuration(("Authorization:Enabled", value))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("Authorization:Authentication")
                         .And.Message.Contains("Xfcc,Token"));

  [TestCase("")]
  [TestCase(" ")]
  public void CertificatesRequireACertificateHeader(string header)
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", "Xfcc"),
                                           ("Identity:CertificateHeader", header))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("CertificateHeader"));

  [Test]
  public void TokensRequireAnAuthorityUnlessTheyAreNotValidated()
  {
    Assert.That(() => Add(Configuration(("Authorization:Authentication", "Token"))),
                Throws.InstanceOf<InvalidOperationException>()
                      .With.Message.Contains("Authority"));
    Assert.That(Add(Configuration(("Authorization:Authentication", "Token"),
                                  ("Identity:ValidateTokens", "false"))),
                Is.True);
  }

  // Ignored, so that authorization can always be disabled, but warned about
  [TestCase("None",
            "Identity:Authority")]
  [TestCase("None",
            "Identity:CertificateHeader")]
  [TestCase("Xfcc",
            "Identity:Authority")]
  [TestCase("Xfcc",
            "Identity:ValidateTokens")]
  [TestCase("Xfcc",
            "Identity:SubjectClaim")]
  [TestCase("Token",
            "Identity:CertificateHeader")]
  public void TheSettingsOfAMethodNotSelectedAreWarnedAbout(string authentication,
                                                            string key)
  {
    var logger = new Mock<ILogger>();

    var values = new List<(string, string)>
                 {
                   ("Authorization:Authentication", authentication),
                   (key, key == "Identity:ValidateTokens"
                           ? "false"
                           : "value"),
                 };
    if (authentication == "Token")
    {
      // So that Token starts without an authority
      values.Add(("Identity:ValidateTokens", "false"));
    }

    Add(new ServiceCollection(),
        Configuration(values.ToArray()),
        logger.Object);

    logger.VerifyLog(LogLevel.Warning,
                     key,
                     Times.Once());
  }

  [Test]
  public void TheSettingsOfTheSelectedMethodsAreNotWarnedAbout()
  {
    var logger = new Mock<ILogger>();

    Add(new ServiceCollection(),
        Configuration(("Authorization:Authentication", "Xfcc,Token"),
                      ("Identity:CertificateHeader", "X-Client-Cert"),
                      ("Identity:ValidateTokens", "false"),
                      ("Identity:SubjectClaim", "sub")),
        logger.Object);

    logger.Verify(x => x.Log(It.IsAny<LogLevel>(),
                             It.IsAny<EventId>(),
                             It.IsAny<It.IsAnyType>(),
                             It.IsAny<Exception>(),
                             It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
                  Times.Never());
  }

  [TestCase("session.owner = request.caller")]
  [TestCase("")]
  public void ARefusedGrantPreventsStarting(string condition)
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", "Xfcc"),
                                           ("Authorization:Grants:0:Id", "broken"),
                                           ("Authorization:Grants:0:Principal", "role:team"),
                                           ("Authorization:Grants:0:PermissionSet", "session-own"),
                                           ("Authorization:PermissionSets:0:Name", "session-own"),
                                           ("Authorization:PermissionSets:0:Permissions:0", "session:read"),
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
    Assert.That(() => Add(Configuration((key, value))),
                Throws.InstanceOf<InvalidOperationException>()
                      .With.Message.Contains(key.Split(':')[0])
                      .And.Message.Contains("Authorization:Authentication"));
    Assert.That(Add(Configuration((key, value),
                                  ("Authorization:Authentication", "None"))),
                Is.False);
  }

  [Test]
  public void AnAuthorityRequiresAnAudience()
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", "Token"),
                                           ("Identity:Authority", "https://idp.example"))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("Audience"));

  [Test]
  public void AnAuthorityIsRefusedWhenTokensAreNotValidated()
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", "Token"),
                                           ("Identity:Authority", "https://idp.example"),
                                           ("Identity:Audience", "armonik"),
                                           ("Identity:ValidateTokens", "false"))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("ValidateTokens"));

  [TestCase("00:00:00")]
  [TestCase("00:00:00.5")]
  public void AMetadataRefreshIntervalUnderOneSecondPreventsStarting(string interval)
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", "Token"),
                                           ("Identity:Authority", "https://idp.example"),
                                           ("Identity:Audience", "armonik"),
                                           ("Identity:MetadataRefreshInterval", interval))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("MetadataRefreshInterval"));

  [Test]
  public void AMetadataRefreshIntervalOfOneSecondIsAccepted()
    => Assert.That(Add(Configuration(("Authorization:Authentication", "Token"),
                                     ("Identity:Authority", "https://idp.example"),
                                     ("Identity:Audience", "armonik"),
                                     ("Identity:MetadataRefreshInterval", "00:00:01"))),
                   Is.True);

  [TestCase("00:00:00")]
  [TestCase("-00:00:00.001")]
  [TestCase("14.00:00:00.001")]
  [TestCase("10675199.02:48:05.4775807")]
  public void AGrantCacheTtlOutOfRangePreventsStarting(string ttl)
    => Assert.That(() => Add(Configuration(("Authorization:Authentication", "Xfcc"),
                                           ("Authorization:GrantCacheTtl", ttl))),
                   Throws.InstanceOf<InvalidOperationException>()
                         .With.Message.Contains("GrantCacheTtl"));

  [TestCase("00:00:00.001")]
  [TestCase("14.00:00:00")]
  public void AGrantCacheTtlInRangeIsAccepted(string ttl)
    => Assert.That(Add(Configuration(("Authorization:Authentication", "Xfcc"),
                                     ("Authorization:GrantCacheTtl", ttl))),
                   Is.True);
}
