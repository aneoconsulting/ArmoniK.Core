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

using System.Collections.Generic;

using Grpc.Core;
using Grpc.Core.Interceptors;

namespace ArmoniK.Samples.HtcMock.Client;

/// <summary>
///   Wraps a channel so every call carries extra metadata.
/// </summary>
internal sealed class ExtraHeaderChannel : ChannelBase
{
  private readonly IReadOnlyList<(string Name, string Value)> headers_;
  private readonly ChannelBase                                inner_;

  /// <summary>Wraps <paramref name="inner" />, attaching <paramref name="headers" /> to every call.</summary>
  public ExtraHeaderChannel(ChannelBase                                inner,
                            IReadOnlyList<(string Name, string Value)> headers)
    : base(inner.Target)
  {
    inner_   = inner;
    headers_ = headers;
  }

  /// <inheritdoc />
  public override CallInvoker CreateCallInvoker()
    => inner_.CreateCallInvoker()
             .Intercept(metadata =>
                        {
                          foreach (var (name, value) in headers_)
                          {
                            metadata.Add(name,
                                         value);
                          }

                          return metadata;
                        });
}
