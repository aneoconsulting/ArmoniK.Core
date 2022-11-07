// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2021-$CURRENT_YEAR$. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using Grpc.Core;

namespace ArmoniK.Core.Common.Tests.Helpers;

public class TestUnavailableRpcException : RpcException
{
  public TestUnavailableRpcException()
    : base(new Status(StatusCode.Unavailable,
                      ""))
  {
  }

  public TestUnavailableRpcException(string message)
    : base(new Status(StatusCode.Unavailable,
                      ""),
           message)
  {
  }

  public TestUnavailableRpcException(Metadata trailers)
    : base(new Status(StatusCode.Unavailable,
                      ""),
           trailers)
  {
  }

  public TestUnavailableRpcException(Metadata trailers,
                                     string   message)
    : base(new Status(StatusCode.Unavailable,
                      ""),
           trailers,
           message)
  {
  }
}
