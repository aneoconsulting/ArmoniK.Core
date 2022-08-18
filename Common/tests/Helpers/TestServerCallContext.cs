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
  private readonly Dictionary<object, object> userState_;

  private TestServerCallContext(Metadata          requestHeaders,
                                CancellationToken cancellationToken)
  {
    RequestHeadersCore    = requestHeaders;
    CancellationTokenCore = cancellationToken;
    ResponseTrailersCore  = new Metadata();
    AuthContextCore = new AuthContext(string.Empty,
                                      new Dictionary<string, List<AuthProperty>>());
    userState_ = new Dictionary<object, object>();
  }

  public Metadata? ResponseHeaders { get; private set; }

  protected override string MethodCore
    => "MethodName";

  protected override string HostCore
    => "HostName";

  protected override string PeerCore
    => "PeerName";

  protected override DateTime DeadlineCore { get; }

  protected override Metadata RequestHeadersCore { get; }

  protected override CancellationToken CancellationTokenCore { get; }

  protected override Metadata ResponseTrailersCore { get; }

  protected override Status StatusCore { get; set; }

  protected override WriteOptions? WriteOptionsCore { get; set; }

  protected override AuthContext AuthContextCore { get; }

  protected override IDictionary<object, object> UserStateCore
    => userState_;

  protected override ContextPropagationToken CreatePropagationTokenCore(ContextPropagationOptions? options)
    => throw new NotImplementedException();

  protected override Task WriteResponseHeadersAsyncCore(Metadata? responseHeaders)
  {
    if (ResponseHeaders != null)
    {
      throw new InvalidOperationException("Response headers have already been written.");
    }

    ResponseHeaders = responseHeaders;
    return Task.CompletedTask;
  }

  public static TestServerCallContext Create(Metadata?         requestHeaders    = null,
                                             CancellationToken cancellationToken = default)
    => new(requestHeaders ?? new Metadata(),
           cancellationToken);
}
