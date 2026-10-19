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
using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Core.Authorization;
using ArmoniK.Core.Common.Enforcement;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Storage;

using Google.Protobuf;

using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Web;

using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using Moq;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.Enforcement;

[TestFixture(TestOf = typeof(CallerAuthenticationMiddleware))]
public class CallerAuthenticationMiddlewareTest
{
  [SetUp]
  public async Task SetUp()
  {
    service_ = new RecordingResults();

    var builder = WebApplication.CreateBuilder();
    builder.Services.AddGrpc();
    builder.Services.AddSingleton(service_)
           .AddSingleton(new AuthorizationEnforcer(Mock.Of<IDecisionEngineProvider>(),
                                                   new CallerResolver(new IdentityOptions()),
                                                   new AuthorizationAudit(NullLoggerFactory.Instance),
                                                   Mock.Of<ISessionTable>(),
                                                   Mock.Of<ITaskTable>(),
                                                   Mock.Of<IResultTable>(),
                                                   new Injection.Options.Submitter(),
                                                   NullLogger<AuthorizationEnforcer>.Instance));
    builder.WebHost.UseTestServer();

    app_ = builder.Build();
    app_.UseRouting();
    app_.UseGrpcWeb();
    app_.UseCallerAuthentication();
    app_.MapGrpcService<RecordingResults>()
        .EnableGrpcWeb();

    await app_.StartAsync()
              .ConfigureAwait(false);
  }

  [TearDown]
  public async Task TearDown()
    => await app_!.DisposeAsync()
                  .ConfigureAwait(false);

  private WebApplication?   app_;
  private RecordingResults? service_;

  /// <summary>Records what reaches the handlers, and the caller left by the middleware</summary>
  private sealed class RecordingResults : Results.ResultsBase
  {
    public object? Caller;
    public int     Calls;

    public override Task<CreateResultsMetaDataResponse> CreateResultsMetaData(CreateResultsMetaDataRequest request,
                                                                              ServerCallContext            context)
    {
      Calls++;
      Caller = context.GetHttpContext()
                      .Items[AuthorizationEnforcer.CallerKey];
      return Task.FromResult(new CreateResultsMetaDataResponse());
    }

    public override async Task<UploadResultDataResponse> UploadResultData(IAsyncStreamReader<UploadResultDataRequest> requestStream,
                                                                          ServerCallContext                           context)
    {
      Calls++;
      while (await requestStream.MoveNext()
                                .ConfigureAwait(false))
      {
      }

      return new UploadResultDataResponse();
    }
  }

  private Results.ResultsClient Client(bool grpcWeb = false)
  {
    var handler = app_!.GetTestServer()
                       .CreateHandler();
    if (grpcWeb)
    {
      handler = new GrpcWebHandler(GrpcWebMode.GrpcWeb,
                                   handler);
    }

    return new Results.ResultsClient(GrpcChannel.ForAddress("http://localhost",
                                                            new GrpcChannelOptions
                                                            {
                                                              HttpHandler = handler,
                                                            }));
  }

  private static Metadata Certificate(string subject)
    => Certificate(new X500DistinguishedName(subject));

  private static Metadata Certificate(X500DistinguishedName subject)
  {
    using var key = RSA.Create(2048);
    using var certificate = new CertificateRequest(subject,
                                                   key,
                                                   HashAlgorithmName.SHA256,
                                                   RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1),
                                                                                               DateTimeOffset.UtcNow.AddDays(1));
    return new Metadata
           {
             {
               "X-Forwarded-Client-Cert", $"Cert=\"{Uri.EscapeDataString(certificate.ExportCertificatePem())}\""
             },
           };
  }

  [Test]
  public void AUnaryRequestWithoutCredentialIsRefusedBeforeTheHandler()
  {
    var exception = Assert.ThrowsAsync<RpcException>(async () => await Client()
                                                                       .CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest())
                                                                       .ConfigureAwait(false));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.Unauthenticated));
    Assert.That(exception.Status.Detail,
                Is.EqualTo("A token or a client certificate is required"));
    Assert.That(service_!.Calls,
                Is.Zero);
  }

  [Test]
  public async Task AStreamWithoutCredentialIsRefusedBeforeItsFirstMessageIsRead()
  {
    using var call = Client()
      .UploadResultData();

    try
    {
      await call.RequestStream.WriteAsync(new UploadResultDataRequest
                                          {
                                            DataChunk = ByteString.CopyFromUtf8("data"),
                                          })
                .ConfigureAwait(false);
      await call.RequestStream.CompleteAsync()
                .ConfigureAwait(false);
    }
    catch (RpcException)
    {
      // The server may have answered before the message was sent
    }

    var exception = Assert.ThrowsAsync<RpcException>(async () => await call.ResponseAsync.ConfigureAwait(false));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.Unauthenticated));
    Assert.That(service_!.Calls,
                Is.Zero);
  }

  [Test]
  public void AnInvalidCredentialIsRefusedWithItsReason()
  {
    var exception = Assert.ThrowsAsync<RpcException>(async () => await Client()
                                                                       .CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest(),
                                                                                                   new Metadata
                                                                                                   {
                                                                                                     {
                                                                                                       "X-Forwarded-Client-Cert", "not a certificate"
                                                                                                     },
                                                                                                   })
                                                                       .ConfigureAwait(false));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.Unauthenticated));
    Assert.That(exception.Status.Detail,
                Is.EqualTo("The forwarded client certificate header has a malformed pair at position 0"));
    Assert.That(service_!.Calls,
                Is.Zero);
  }

  [Test]
  public void AGrpcWebRequestWithoutCredentialIsRefusedBeforeTheHandler()
  {
    var exception = Assert.ThrowsAsync<RpcException>(async () => await Client(true)
                                                                       .CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest())
                                                                       .ConfigureAwait(false));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.Unauthenticated));
    Assert.That(service_!.Calls,
                Is.Zero);
  }

  [Test]
  public async Task AnAuthenticatedCallerReachesTheHandlerWithTheCallerKept()
  {
    await Client()
          .CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest(),
                                      Certificate("CN=alice, OU=modelling-team"))
          .ConfigureAwait(false);

    Assert.That(service_!.Calls,
                Is.EqualTo(1));
    Assert.That(service_.Caller,
                Is.InstanceOf<AuthenticatedCaller>());
  }

  [Test]
  public void ACertificateWhoseSubjectCannotBeDecodedIsUnauthenticated()
  {
    // CN as a PrintableString holding '_', which is not a PrintableString character
    var writer = new AsnWriter(AsnEncodingRules.DER);
    using (writer.PushSequence())
    using (writer.PushSetOf())
    using (writer.PushSequence())
    {
      writer.WriteObjectIdentifier("2.5.4.3");
      // PrintableString tag, length, value
      writer.WriteEncodedValue([0x13, 0x03, (byte)'a', (byte)'_', (byte)'b']);
    }

    var exception = Assert.ThrowsAsync<RpcException>(async () => await Client()
                                                                       .CreateResultsMetaDataAsync(new CreateResultsMetaDataRequest(),
                                                                                                   Certificate(new X500DistinguishedName(writer.Encode())))
                                                                       .ConfigureAwait(false));

    Assert.That(exception!.StatusCode,
                Is.EqualTo(StatusCode.Unauthenticated));
    Assert.That(exception.Status.Detail,
                Does.StartWith("The client certificate subject cannot be decoded"));
    Assert.That(service_!.Calls,
                Is.Zero);
  }
}
