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
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Grpc.Core;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestServerCallContext : ServerCallContext
{
  private readonly Metadata                   requestHeaders_;
  private readonly CancellationToken          cancellationToken_;
  private readonly Metadata                   responseTrailers_;
  private readonly AuthContext                authContext_;
  private readonly Dictionary<object, object> userState_;
  private          WriteOptions              writeOptions_;

  public Metadata ResponseHeaders { get; private set; }

  private TestServerCallContext(Metadata          requestHeaders,
                                CancellationToken cancellationToken)
  {
    requestHeaders_    = requestHeaders;
    cancellationToken_ = cancellationToken;
    responseTrailers_  = new Metadata();
    authContext_ = new AuthContext(string.Empty,
                                   new Dictionary<string, List<AuthProperty>>());
    userState_ = new Dictionary<object, object>();
  }

  protected override string MethodCore
    => "MethodName";

  protected override string HostCore
    => "HostName";

  protected override string PeerCore
    => "PeerName";

  protected override DateTime DeadlineCore { get; }

  protected override Metadata RequestHeadersCore
    => requestHeaders_;

  protected override CancellationToken CancellationTokenCore
    => cancellationToken_;

  protected override Metadata ResponseTrailersCore
    => responseTrailers_;

  protected override Status StatusCore { get; set; }

  protected override WriteOptions WriteOptionsCore
  {
    get => writeOptions_;
    set { writeOptions_ = value; }
  }

  protected override AuthContext AuthContextCore
    => authContext_;

  protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions options)
  {
    throw new NotImplementedException();
  }

  protected override Task WriteResponseHeadersAsyncCore(Metadata responseHeaders)
  {
    if (ResponseHeaders != null)
    {
      throw new InvalidOperationException("Response headers have already been written.");
    }

    ResponseHeaders = responseHeaders;
    return Task.CompletedTask;
  }

  protected override IDictionary<object, object> UserStateCore
    => userState_;

  public static TestServerCallContext Create(Metadata         requestHeaders    = null,
                                             CancellationToken cancellationToken = default)
  {
    return new TestServerCallContext(requestHeaders ?? new Metadata(),
                                     cancellationToken);
  }
}
