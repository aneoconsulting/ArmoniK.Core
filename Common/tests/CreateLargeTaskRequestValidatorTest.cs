// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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
using System.Text;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Validators;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(CreateLargeTaskRequestValidator))]
public class CreateLargeTaskRequestValidatorTest
{
  private readonly CreateLargeTaskRequestValidator validator_ = new();

  [Test]
  public void CompleteInitRequestShouldBeValid()
  {
    var ctr = new CreateLargeTaskRequest()
    {
      InitRequest = new CreateLargeTaskRequest.Types.InitRequest
      {
        TaskOptions = new TaskOptions
        {
          MaxDuration = Duration.FromTimeSpan(TimeSpan.Zero),
          MaxRetries  = 3,
          Priority    = 1,
        },
        SessionId = "SessionId",
      },
    };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void MissingSessionIdShouldFail()
  {
    var ctr = new CreateLargeTaskRequest()
    {
      InitRequest = new CreateLargeTaskRequest.Types.InitRequest
      {
        TaskOptions = new TaskOptions
        {
          MaxDuration = Duration.FromTimeSpan(TimeSpan.Zero),
          MaxRetries  = 3,
          Priority    = 1,
        },
      },
    };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void BlankSessionShouldFail()
  {
    var ctr = new CreateLargeTaskRequest()
    {
      InitRequest = new CreateLargeTaskRequest.Types.InitRequest
      {
        TaskOptions = new TaskOptions
        {
          MaxDuration = Duration.FromTimeSpan(TimeSpan.Zero),
          MaxRetries  = 3,
          Priority    = 1,
        },
        SessionId = "    ",
      },
    };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void NullPayloadShouldFail()
  {
    var ctr = new CreateLargeTaskRequest()
    {
      TaskPayload = null,
    };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void EmptyDataChunkShouldFail()
  {
    var ctr = new CreateLargeTaskRequest()
    {
      TaskPayload = new DataChunk(),
    };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void NullDataInDataChunkShouldThrowError()
  {
    Assert.Throws<ArgumentNullException>(() => new CreateLargeTaskRequest()
    {
      TaskPayload = new DataChunk
      {
        Data = null,
      },
    });
  }

  [Test]
  public void EmptyDataInDataChunkShouldSucceed()
  {
    var ctr = new CreateLargeTaskRequest()
    {
      TaskPayload = new DataChunk
      {
        Data = ByteString.Empty,
      },
    };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void LargeDataInDataChunkShouldSucceed()
  {
    Random rnd       = new Random();
    Console.WriteLine(PayloadConfiguration.MaxChunkSize);
    byte[] dataBytes = new byte[PayloadConfiguration.MaxChunkSize];
    rnd.NextBytes(dataBytes);
    var byteString = ByteString.CopyFrom(dataBytes);
    Console.WriteLine(byteString.Length);

    var ctr = new CreateLargeTaskRequest()
    {
      TaskPayload = new DataChunk
      {
        Data = byteString,
      },
    };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void TooLargeDataInDataChunkShouldFail()
  {
    Random rnd = new Random();
    Console.WriteLine(PayloadConfiguration.MaxChunkSize);
    byte[] dataBytes = new byte[PayloadConfiguration.MaxChunkSize + 100];
    rnd.NextBytes(dataBytes);
    var byteString = ByteString.CopyFrom(dataBytes);
    Console.WriteLine(byteString.Length);

    var ctr = new CreateLargeTaskRequest()
    {
      TaskPayload = new DataChunk
      {
        Data = byteString,
      },
    };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }
}