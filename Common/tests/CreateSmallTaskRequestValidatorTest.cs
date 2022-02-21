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

[TestFixture(TestOf = typeof(CreateSmallTaskRequestValidator))]
public class CreateSmallTaskRequestValidatorTest
{
  private readonly CreateSmallTaskRequestValidator validator_ = new();

  [Test]
  public void CompleteRequestShouldBeValid()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                           Encoding.ASCII),
                  },
                },
              };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void MissingSessionIdShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void EmptySessionShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = string.Empty,
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void BlankSessionShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "      ",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void UndefinedTaskOptionShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void UndefinedMaxDurationShouldBeValid()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxRetries = 1,
                                Priority   = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void UndefinedMaxRetriesShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void ZeroMaxRetryShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 0,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void NegativeMaxRetryShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = -6,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }


  [Test]
  public void UndefinedOptionsShouldBeValid()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void UndefinedPriorityShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void ZeroPriorityShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 0,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void NegativePriorityShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = -6,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void TooBigPriorityShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 100,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void OnlyMaxRetryAndPriorityDefinedShouldBeValid()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxRetries = 1,
                                Priority   = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.CopyFrom("payload",
                                                  Encoding.ASCII),
                  },
                },
              };

    Assert.IsTrue(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void UndefinedTaskRequestShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void EmptyTaskRequestShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void UndefinedPayloadShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest(),
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }

  [Test]
  public void EmptyPayloadShouldFail()
  {
    var ctr = new CreateSmallTaskRequest
              {
                SessionId = "Session",
                TaskOptions = new()
                              {
                                MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                MaxRetries  = 1,
                                Priority    = 1,
                              },
                TaskRequests =
                {
                  new TaskRequest
                  {
                    Payload = ByteString.Empty,
                  },
                },
              };

    Assert.IsFalse(validator_.Validate(ctr).IsValid);
  }
}