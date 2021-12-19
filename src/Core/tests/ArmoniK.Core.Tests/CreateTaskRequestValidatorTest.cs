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

using ArmoniK.Core.gRPC.V1;
using ArmoniK.Core.gRPC.Validators;
using ArmoniK.Core.Storage;

using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using NUnit.Framework;

namespace ArmoniK.Core.Tests
{
  [TestFixture(TestOf = typeof(CreateTaskRequestValidator))]
  public class CreateTaskRequestValidatorTest
  {
    private readonly CreateTaskRequestValidator validator_ = new();

    [Test]
    public void CompleteRequestShouldBeValid()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsTrue(validator_.Validate(ctr).IsValid);
    }

    #region SessionId

    [Test]
    public void MissingSessionIdShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void MissingSessionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void EmptySessionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = string.Empty,
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void BlankSessionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "      ",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void MissingSubSessionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session = "Session",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void EmptySubSessionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = string.Empty,
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void BlankSubSessionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "      ",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    #endregion

    #region TaskOptions

    [Test]
    public void EmptyIdTadShouldBeValid()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    } 

    [Test]
    public void UndefinedTaskOptionShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = string.Empty,
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsTrue(validator_.Validate(ctr).IsValid);
    } 
    
    [Test]
    public void UndefinedIdTadShouldBeValid()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsTrue(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void UndefinedMaxDurationShouldBeValid()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsTrue(validator_.Validate(ctr).IsValid);
    }
    
    [Test]
    public void UndefinedMaxRetriesShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }
    
    [Test]
    public void ZeroMaxRetryShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 0,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void NegativeMaxRetryShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = -6,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }


    [Test]
    public void UndefinedOptionsShouldBeValid()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsTrue(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void UndefinedPriorityShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void ZeroPriorityShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority = 0,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void NegativePriorityShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority = -6,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void TooBigPriorityShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority = 100,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void OnlyMaxRetryAndPriorityDefinedShouldBeValid()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  MaxRetries  = 1,
                                  Priority = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom("payload",
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsTrue(validator_.Validate(ctr).IsValid);
    }

    #endregion

    #region TaskRequest

    [Test]
    public void UndefinedTaskRequestShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void EmptyTaskRequestShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void UndefinedPayloadShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    [Test]
    public void EmptyPayloadShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }
    [Test]
    public void EmptyDataShouldFail()
    {
      var ctr = new CreateTaskRequest
                {
                  SessionId = new SessionId
                              {
                                Session    = "Session",
                                SubSession = "SubSession",
                              },
                  TaskOptions = new TaskOptions
                                {
                                  IdTag       = "Tag",
                                  MaxDuration = Duration.FromTimeSpan(TimeSpan.MinValue),
                                  MaxRetries  = 1,
                                  Options     = { },
                                  Priority    = 1,
                                },
                  TaskRequests =
                  {
                    new TaskRequest
                    {
                      Payload = new Payload
                                {
                                  Data = ByteString.CopyFrom(string.Empty,
                                                             Encoding.ASCII),
                                },
                      DependenciesTaskIds = { },
                    },
                  },
                };

      Assert.IsFalse(validator_.Validate(ctr).IsValid);
    }

    #endregion

  }
}
