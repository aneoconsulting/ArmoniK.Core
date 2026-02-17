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
using System.Collections.Generic;

using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;

using NUnit.Framework;

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

    Assert.That(deserialized.SessionId,
                Is.EqualTo(rdm.SessionId));
    Assert.That(deserialized.PartitionIds,
                Is.EqualTo(rdm.PartitionIds));
    Assert.That(deserialized.Options,
                Is.Not.Null);
    Assert.That(deserialized.Options.MaxDuration,
                Is.EqualTo(rdm.Options.MaxDuration));
    Assert.That(deserialized.Options.MaxRetries,
                Is.EqualTo(rdm.Options.MaxRetries));
    Assert.That(deserialized.Options.Priority,
                Is.EqualTo(rdm.Options.Priority));
  }

  [Test]
  public void SerializeResultDataModel()
  {
    var rdm = new Result("sessionId",
                         "ResultId",
                         "Name",
                         "CreatedBy",
                         "CompletedBy",
                         "OwnerTaskId",
                         ResultStatus.Completed,
                         new List<string>
                         {
                           "Task1",
                           "Task2",
                         },
                         DateTime.Parse("2022-02-15 8:55:05.954")
                                 .ToUniversalTime(),
                         DateTime.Parse("2025-04-01 15:04:36.723")
                                 .ToUniversalTime(),
                         3,
                         new[]
                         {
                           (byte)1,
                           (byte)2,
                           (byte)3,
                         },
                         false);

    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<Result>(serialized);

    Assert.That(deserialized.ResultId,
                Is.EqualTo(rdm.ResultId));
    Assert.That(deserialized.SessionId,
                Is.EqualTo(rdm.SessionId));
    Assert.That(deserialized.Name,
                Is.EqualTo(rdm.Name));
    Assert.That(deserialized.CreatedBy,
                Is.EqualTo(rdm.CreatedBy));
    Assert.That(deserialized.CompletedBy,
                Is.EqualTo(rdm.CompletedBy));
    Assert.That(deserialized.OwnerTaskId,
                Is.EqualTo(rdm.OwnerTaskId));
    Assert.That(deserialized.Status,
                Is.EqualTo(rdm.Status));
    Assert.That(deserialized.DependentTasks,
                Is.EqualTo(new List<string>
                           {
                             "Task1",
                             "Task2",
                           }));
    Assert.That(deserialized.CreationDate,
                Is.EqualTo(rdm.CreationDate));
    Assert.That(deserialized.CompletionDate,
                Is.EqualTo(rdm.CompletionDate));
    Assert.That(rdm.OpaqueId,
                Is.EqualTo(deserialized.OpaqueId));
    Assert.That(deserialized.ManualDeletion,
                Is.EqualTo(rdm.ManualDeletion));
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
                           "createdBy",
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
                           DateTime.Today.ToUniversalTime(),
                           DateTime.Today.ToUniversalTime(),
                           TimeSpan.FromSeconds(1),
                           TimeSpan.FromSeconds(2),
                           TimeSpan.FromSeconds(3),
                           new Output(OutputStatus.Error,
                                      "Output Message"));

    var serialized = tdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<TaskData>(serialized);

    Assert.That(deserialized,
                Is.Not.Null);
    Assert.That(deserialized.Options,
                Is.Not.Null);
    Assert.That(deserialized.Options.Priority,
                Is.EqualTo(tdm.Options.Priority));
    Assert.That(deserialized.Options.PartitionId,
                Is.EqualTo(tdm.Options.PartitionId));
    Assert.That(tdm.Options.Options,
                Is.Not.Null);
    Assert.That(deserialized.Options.Options["key1"],
                Is.EqualTo(tdm.Options.Options["key1"]));
    Assert.That(deserialized.Options.Options["key2"],
                Is.EqualTo(tdm.Options.Options["key2"]));
    Assert.That(tdm.DataDependencies,
                Is.EqualTo(deserialized.DataDependencies));
    Assert.That(tdm.RemainingDataDependencies,
                Is.EqualTo(deserialized.RemainingDataDependencies));
    Assert.That(deserialized.Options.MaxDuration,
                Is.EqualTo(tdm.Options.MaxDuration));
    Assert.That(deserialized.Options.MaxRetries,
                Is.EqualTo(tdm.Options.MaxRetries));
    Assert.That(deserialized.TaskId,
                Is.EqualTo(tdm.TaskId));
    Assert.That(deserialized.Status,
                Is.EqualTo(tdm.Status));
    Assert.That(deserialized.SessionId,
                Is.EqualTo(tdm.SessionId));
    Assert.That(deserialized.OwnerPodId,
                Is.EqualTo(tdm.OwnerPodId));
    Assert.That(deserialized.SubmittedDate,
                Is.EqualTo(tdm.SubmittedDate));
    Assert.That(deserialized.CreationDate,
                Is.EqualTo(tdm.CreationDate));
    Assert.That(deserialized.ReceptionDate,
                Is.EqualTo(tdm.ReceptionDate));
    Assert.That(deserialized.AcquisitionDate,
                Is.EqualTo(tdm.AcquisitionDate));
    Assert.That(deserialized.CreationToEndDuration,
                Is.EqualTo(tdm.CreationToEndDuration));
    Assert.That(deserialized.ProcessingToEndDuration,
                Is.EqualTo(tdm.ProcessingToEndDuration));
    Assert.That(tdm.RetryOfIds,
                Is.EqualTo(deserialized.RetryOfIds));
    Assert.That(tdm.ExpectedOutputIds,
                Is.EqualTo(deserialized.ExpectedOutputIds));
    Assert.That(tdm.ParentTaskIds,
                Is.EqualTo(deserialized.ParentTaskIds));
  }

  [Test]
  public void SerializeUserDataModel()
  {
    var udm = new UserData(0,
                           "Username",
                           new[]
                           {
                             0,
                             1,
                           });
    var serialized = udm.ToBson();

    var deserialized = BsonSerializer.Deserialize<UserData>(serialized);

    Assert.That(deserialized,
                Is.Not.Null);
    Assert.That(deserialized.UserId,
                Is.EqualTo(udm.UserId));
    Assert.That(deserialized.Username,
                Is.EqualTo(udm.Username));
    Assert.That(deserialized.Roles,
                Is.Not.Null);
    Assert.That(deserialized.Roles,
                Is.EqualTo(udm.Roles));
  }

  [Test]
  public void SerializeRoleDataModel()
  {
    var rdm = new RoleData(0,
                           "RoleName",
                           new[]
                           {
                             "cat1:name1",
                             "cat2:name2:*",
                           });
    var serialized = rdm.ToBson();

    var deserialized = BsonSerializer.Deserialize<RoleData>(serialized);

    Assert.That(deserialized,
                Is.Not.Null);
    Assert.That(deserialized.RoleId,
                Is.EqualTo(rdm.RoleId));
    Assert.That(deserialized.RoleName,
                Is.EqualTo(rdm.RoleName));
    Assert.That(deserialized.Permissions,
                Is.Not.Null);
    Assert.That(deserialized.Permissions,
                Is.EqualTo(rdm.Permissions));
  }

  [Test]
  public void SerializeAuthDataModel()
  {
    var adm = new AuthData(1,
                           1,
                           "CN",
                           "Fingerprint");
    var serialized = adm.ToBson();

    Console.WriteLine(adm.ToBsonDocument());

    var deserialized = BsonSerializer.Deserialize<AuthData>(serialized);

    Assert.That(deserialized,
                Is.Not.Null);
    Assert.That(deserialized.AuthId,
                Is.EqualTo(adm.AuthId));
    Assert.That(deserialized.UserId,
                Is.EqualTo(adm.UserId));
    Assert.That(deserialized.Cn,
                Is.EqualTo(adm.Cn));
    Assert.That(deserialized.Fingerprint,
                Is.EqualTo(adm.Fingerprint));
  }

  [Test]
  public void SerializeUserAuthenticationResult()
  {
    var uirm = new UserAuthenticationResult(0,
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
    Assert.That(deserialized,
                Is.Not.Null);
    Assert.That(deserialized.Id,
                Is.EqualTo(uirm.Id));
    Assert.That(deserialized.Username,
                Is.EqualTo(uirm.Username));
    Assert.That(deserialized.Roles,
                Is.Not.Null);
    Assert.That(uirm.Roles,
                Is.EqualTo(deserialized.Roles));
    Assert.That(deserialized.Permissions,
                Is.Not.Null);
    Assert.That(uirm.Permissions,
                Is.EqualTo(deserialized.Permissions));
  }

  [Test]
  public void InitializeResultDataModelMapping()
  {
    _ = new ResultDataModelMapping();
    Assert.That(BsonClassMap.IsClassMapRegistered(typeof(Result)),
                Is.True);
  }

  [Test]
  public void InitializeTaskDataModelMapping()
  {
    _ = new TaskDataModelMapping();
    Assert.That(BsonClassMap.IsClassMapRegistered(typeof(TaskData)),
                Is.True);
  }


  [Test]
  public void InitializeSessionDataModelMapping()
  {
    _ = new SessionDataModelMapping();
    Assert.That(BsonClassMap.IsClassMapRegistered(typeof(SessionData)),
                Is.True);
  }

  [Test]
  public void InitializeUserDataModelMapping()
  {
    _ = new UserDataModelMapping();
    Assert.That(BsonClassMap.IsClassMapRegistered(typeof(UserData)),
                Is.True);
  }

  [Test]
  public void InitializeRoleDataModelMapping()
  {
    _ = new RoleDataModelMapping();
    Assert.That(BsonClassMap.IsClassMapRegistered(typeof(RoleData)),
                Is.True);
  }

  [Test]
  public void InitializeAuthDataModelMapping()
  {
    _ = new AuthDataModelMapping();
    Assert.That(BsonClassMap.IsClassMapRegistered(typeof(AuthData)),
                Is.True);
  }
}
