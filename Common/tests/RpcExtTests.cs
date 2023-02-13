// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2023. All rights reserved.
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

using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;

using Grpc.Core;

using NUnit.Framework;

using TimeoutException = ArmoniK.Core.Common.Exceptions.TimeoutException;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(RpcExt))]
public class RpcExtTests
{
  [Test]
  [TestCase(StatusCode.OK)]
  [TestCase(StatusCode.Unknown)]
  [TestCase(StatusCode.Internal)]
  [TestCase(StatusCode.InvalidArgument)]
  [TestCase(StatusCode.NotFound)]
  [TestCase(StatusCode.AlreadyExists)]
  [TestCase(StatusCode.PermissionDenied)]
  [TestCase(StatusCode.Unauthenticated)]
  [TestCase(StatusCode.ResourceExhausted)]
  [TestCase(StatusCode.FailedPrecondition)]
  [TestCase(StatusCode.AlreadyExists)]
  [TestCase(StatusCode.OutOfRange)]
  [TestCase(StatusCode.Unimplemented)]
  [TestCase(StatusCode.Internal)]
  [TestCase(StatusCode.Unavailable)]
  [TestCase(StatusCode.DataLoss)]
  public void HandleRpcExceptionsWithStatus(StatusCode status)
    => Assert.IsTrue(RpcExt.HandleExceptions(new RpcException(Status.DefaultSuccess),
                                             status));

  [Test]
  [TestCase(StatusCode.OK)]
  [TestCase(StatusCode.Unknown)]
  [TestCase(StatusCode.Internal)]
  [TestCase(StatusCode.InvalidArgument)]
  [TestCase(StatusCode.NotFound)]
  [TestCase(StatusCode.AlreadyExists)]
  [TestCase(StatusCode.PermissionDenied)]
  [TestCase(StatusCode.Unauthenticated)]
  [TestCase(StatusCode.ResourceExhausted)]
  [TestCase(StatusCode.FailedPrecondition)]
  [TestCase(StatusCode.AlreadyExists)]
  [TestCase(StatusCode.OutOfRange)]
  [TestCase(StatusCode.Unimplemented)]
  [TestCase(StatusCode.Internal)]
  [TestCase(StatusCode.Unavailable)]
  [TestCase(StatusCode.DataLoss)]
  public void HandleAggregateExceptionsWithStatus(StatusCode status)
    => Assert.IsTrue(RpcExt.HandleExceptions(new AggregateException(new RpcException(Status.DefaultSuccess)),
                                             status));


  [Test]
  public void ThrowCancelledExceptionWhenCallWasCancelled()
    => Assert.Throws<TaskCanceledException>(() => RpcExt.HandleExceptions(new RpcException(Status.DefaultSuccess),
                                                                          StatusCode.Cancelled));

  [Test]
  public void ThrowTimeoutExceptionWhenCallReachedDeadline()
    => Assert.Throws<TimeoutException>(() => RpcExt.HandleExceptions(new RpcException(Status.DefaultSuccess),
                                                                     StatusCode.DeadlineExceeded));

  [Test]
  public void ThrowCancelledExceptionWhenCallWasCancelledAggregated()
    => Assert.Throws<TaskCanceledException>(() => RpcExt.HandleExceptions(new AggregateException(new RpcException(Status.DefaultSuccess)),
                                                                          StatusCode.Cancelled));

  [Test]
  public void ThrowTimeoutExceptionWhenCallReachedDeadlineAggregated()
    => Assert.Throws<TimeoutException>(() => RpcExt.HandleExceptions(new AggregateException(new RpcException(Status.DefaultSuccess)),
                                                                     StatusCode.DeadlineExceeded));
}
