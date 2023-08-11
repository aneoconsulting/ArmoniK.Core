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
using System.Collections.Generic;
using System.Linq;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using NUnit.Framework;

using Output = ArmoniK.Core.Common.Storage.Output;
using TaskOptions = ArmoniK.Core.Base.DataStructures.TaskOptions;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

[TestFixture]
internal class BsonSerializerTest
{
  [Test]
  public void SerializeSessionDataModel()
  {
    var rdm = new SessionData("SessionId",
                              SessionStatus.Running,
                              new[]
                              {
                                "part1",
                                "part2",
                              },
                              new TaskOptions(new Dictionary<string, string>(),
                                              TimeSpan.FromHours(1),
                                              2,
                                              1,
                                              "part1",
                                              "ApplicationName2",
                                              "ApplicationVersion2",
                                              "",
                                              "",
                                              ""));

    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<SessionData>(serialized);

    Assert.AreEqual(rdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(rdm.PartitionIds,
                    deserialized.PartitionIds);
    Assert.IsNotNull(deserialized.Options);
    Assert.AreEqual(rdm.Options.MaxDuration,
                    deserialized.Options.MaxDuration);
    Assert.AreEqual(rdm.Options.MaxRetries,
                    deserialized.Options.MaxRetries);
    Assert.AreEqual(rdm.Options.Priority,
                    deserialized.Options.Priority);
  }

  [Test]
  public void SerializeResultDataModel()
  {
    var rdm = new Result("sessionId",
                         "ResultId",
                         "Name",
                         "OwnerTaskId",
                         ResultStatus.Completed,
                         new List<string>
                         {
                           "Task1",
                           "Task2",
                         },
                         DateTime.Parse("2022-02-15 8:55:05.954")
                                 .ToUniversalTime(),
                         new[]
                         {
                           (byte)1,
                           (byte)2,
                           (byte)3,
                         });

    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<Result>(serialized);

    Assert.AreEqual(rdm.ResultId,
                    deserialized.ResultId);
    Assert.AreEqual(rdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(rdm.Name,
                    deserialized.Name);
    Assert.AreEqual(rdm.OwnerTaskId,
                    deserialized.OwnerTaskId);
    Assert.AreEqual(rdm.Status,
                    deserialized.Status);
    Assert.AreEqual(new List<string>
                    {
                      "Task1",
                      "Task2",
                    },
                    deserialized.DependentTasks);
    Assert.AreEqual(rdm.CreationDate,
                    deserialized.CreationDate);
    Assert.IsTrue(rdm.Data.SequenceEqual(deserialized.Data));
  }

  [Test]
  public void SerializeTaskDataModel()
  {
    var tdm = new TaskData("SessionId",
                           "TaskId",
                           "ownerPodId",
                           "ownerPodName",
                           "payload",
                           new List<string>
                           {
                             "parent1",
                           },
                           new List<string>
                           {
                             "dependency1",
                           },
                           new Dictionary<string, bool>
                           {
                             {
                               "dependency1", true
                             },
                           },
                           new List<string>
                           {
                             "output1",
                           },
                           "taskId",
                           new List<string>
                           {
                             "retry1",
                             "retry2",
                           },
                           TaskStatus.Submitted,
                           "",
                           new TaskOptions(new Dictionary<string, string>
                                           {
                                             {
                                               "key1", "value1"
                                             },
                                             {
                                               "key2", "value2"
                                             },
                                           },
                                           TimeSpan.FromSeconds(200),
                                           5,
                                           1,
                                           "part1",
                                           "applicationName",
                                           "applicationVersion",
                                           "applicationNamespace",
                                           "applicationService",
                                           "engineType"),
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           TimeSpan.FromSeconds(1),
                           TimeSpan.FromSeconds(2),
                           new Output(false,
                                      "Output Message"));

    var serialized = tdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<TaskData>(serialized);

    Assert.IsNotNull(deserialized);
    Assert.IsNotNull(deserialized.Options);
    Assert.AreEqual(tdm.Options.Priority,
                    deserialized.Options.Priority);
    Assert.AreEqual(tdm.Options.PartitionId,
                    deserialized.Options.PartitionId);
    Assert.IsNotNull(tdm.Options.Options);
    Assert.AreEqual(tdm.Options.Options["key1"],
                    deserialized.Options.Options["key1"]);
    Assert.AreEqual(tdm.Options.Options["key2"],
                    deserialized.Options.Options["key2"]);
    Assert.IsTrue(tdm.DataDependencies.SequenceEqual(deserialized.DataDependencies));
    Assert.IsTrue(tdm.RemainingDataDependencies.SequenceEqual(deserialized.RemainingDataDependencies));
    Assert.AreEqual(tdm.Options.MaxDuration,
                    deserialized.Options.MaxDuration);
    Assert.AreEqual(tdm.Options.MaxRetries,
                    deserialized.Options.MaxRetries);
    Assert.AreEqual(tdm.TaskId,
                    deserialized.TaskId);
    Assert.AreEqual(tdm.Status,
                    deserialized.Status);
    Assert.AreEqual(tdm.SessionId,
                    deserialized.SessionId);
    Assert.AreEqual(tdm.OwnerPodId,
                    deserialized.OwnerPodId);
    Assert.AreEqual(tdm.SubmittedDate,
                    deserialized.SubmittedDate);
    Assert.AreEqual(tdm.CreationDate,
                    deserialized.CreationDate);
    Assert.AreEqual(tdm.ReceptionDate,
                    deserialized.ReceptionDate);
    Assert.AreEqual(tdm.AcquisitionDate,
                    deserialized.AcquisitionDate);
    Assert.AreEqual(tdm.CreationToEndDuration,
                    deserialized.CreationToEndDuration);
    Assert.AreEqual(tdm.ProcessingToEndDuration,
                    deserialized.ProcessingToEndDuration);
    Assert.IsTrue(tdm.RetryOfIds.SequenceEqual(deserialized.RetryOfIds));
    Assert.IsTrue(tdm.ExpectedOutputIds.SequenceEqual(deserialized.ExpectedOutputIds));
    Assert.IsTrue(tdm.ParentTaskIds.SequenceEqual(deserialized.ParentTaskIds));
  }

  [Test]
  public void SerializeUserDataModel()
  {
    var udm = new UserData("UserId".ToOidString(),
                           "Username",
                           new[]
                           {
                             "RoleId1".ToOidString(),
                             "RoleId2".ToOidString(),
                           });
    var serialized = udm.ToBson();

    var deserialized = BsonSerializer.Deserialize<UserData>(serialized);

    Assert.IsNotNull(deserialized);
    Assert.AreEqual(udm.UserId,
                    deserialized.UserId);
    Assert.AreEqual(udm.Username,
                    deserialized.Username);
    Assert.IsNotNull(deserialized.Roles);
    Assert.AreEqual(udm.Roles.Length,
                    deserialized.Roles.Length);
    Assert.AreEqual(udm.Roles[0],
                    deserialized.Roles[0]);
    Assert.AreEqual(udm.Roles[1],
                    deserialized.Roles[1]);
  }

  [Test]
  public void SerializeRoleDataModel()
  {
    var rdm = new RoleData("RoleId".ToOidString(),
                           "RoleName",
                           new[]
                           {
                             "cat1:name1",
                             "cat2:name2:*",
                           });
    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<RoleData>(serialized);

    Assert.IsNotNull(deserialized);
    Assert.AreEqual(rdm.RoleId,
                    deserialized.RoleId);
    Assert.AreEqual(rdm.RoleName,
                    deserialized.RoleName);
    Assert.IsNotNull(deserialized.Permissions);
    Assert.AreEqual(rdm.Permissions.Length,
                    deserialized.Permissions.Length);
    Assert.AreEqual(rdm.Permissions[0],
                    deserialized.Permissions[0]);
    Assert.AreEqual(rdm.Permissions[1],
                    deserialized.Permissions[1]);
  }

  [Test]
  public void SerializeAuthDataModel()
  {
    var adm = new AuthData("AuthId".ToOidString(),
                           "UserId".ToOidString(),
                           "CN",
                           "Fingerprint");
    var serialized = adm.ToBson();

    Console.WriteLine(adm.ToBsonDocument());

    var deserialized = BsonSerializer.Deserialize<AuthData>(serialized);

    Assert.IsNotNull(deserialized);
    Assert.AreEqual(adm.AuthId,
                    deserialized.AuthId);
    Assert.AreEqual(adm.UserId,
                    deserialized.UserId);
    Assert.AreEqual(adm.Cn,
                    deserialized.Cn);
    Assert.AreEqual(adm.Fingerprint,
                    deserialized.Fingerprint);
  }

  [Test]
  public void SerializeUserAuthenticationResult()
  {
    var uirm = new UserAuthenticationResult("Id".ToOidString(),
                                            "Username",
                                            new[]
                                            {
                                              "RoleName1",
                                              "RoleName2",
                                            },
                                            new[]
                                            {
                                              "Permission1:test",
                                              "Permission2:test:*",
                                            });
    var serialized   = uirm.ToBson();
    var deserialized = BsonSerializer.Deserialize<UserAuthenticationResult>(serialized);
    Assert.IsNotNull(deserialized);
    Assert.AreEqual(uirm.Id,
                    deserialized.Id);
    Assert.AreEqual(uirm.Username,
                    deserialized.Username);
    Assert.IsNotNull(deserialized.Roles);
    Assert.AreEqual(uirm.Roles.Count(),
                    deserialized.Roles.Count());
    Assert.IsTrue(uirm.Roles.ToList()
                      .All(s => deserialized.Roles.Contains(s)));
    Assert.IsNotNull(deserialized.Permissions);
    Assert.AreEqual(uirm.Permissions.Count(),
                    deserialized.Permissions.Count());
    Assert.IsTrue(uirm.Permissions.ToList()
                      .All(s => deserialized.Permissions.Contains(s)));
  }

  [Test]
  public void InitializeResultDataModelMapping()
  {
    _ = new ResultDataModelMapping();
    Assert.IsTrue(BsonClassMap.IsClassMapRegistered(typeof(Result)));
  }

  [Test]
  public void InitializeTaskDataModelMapping()
  {
    _ = new TaskDataModelMapping();
    Assert.IsTrue(BsonClassMap.IsClassMapRegistered(typeof(TaskData)));
  }


  [Test]
  public void InitializeSessionDataModelMapping()
  {
    _ = new SessionDataModelMapping();
    Assert.IsTrue(BsonClassMap.IsClassMapRegistered(typeof(SessionData)));
  }

  [Test]
  public void InitializeUserDataModelMapping()
  {
    _ = new UserDataModelMapping();
    Assert.IsTrue(BsonClassMap.IsClassMapRegistered(typeof(UserData)));
  }

  [Test]
  public void InitializeRoleDataModelMapping()
  {
    _ = new RoleDataModelMapping();
    Assert.IsTrue(BsonClassMap.IsClassMapRegistered(typeof(RoleData)));
  }

  [Test]
  public void InitializeAuthDataModelMapping()
  {
    _ = new AuthDataModelMapping();
    Assert.IsTrue(BsonClassMap.IsClassMapRegistered(typeof(AuthData)));
  }
}
