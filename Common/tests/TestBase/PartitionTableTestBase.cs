// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Core.Base.DataStructures;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Core.Common.Tests.ListPartitionsRequestExt;

using Microsoft.Extensions.Diagnostics.HealthChecks;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests.TestBase;

[TestFixture]
public class PartitionTableTestBase
{
  [SetUp]
  public async Task SetUp()
  {
    Environment.SetEnvironmentVariable(PartitionPath.Replace(":",
                                                             "__") + "__0",
                                       PartitionEnv.ToJson());

    GetPartitionTableInstance();

    if (!RunTests || CheckForSkipSetup())
    {
      return;
    }

    await PartitionTable!.Init(CancellationToken.None)
                         .ConfigureAwait(false);

    await PartitionTable!.CreatePartitionsAsync(new[]
                                                {
                                                  new PartitionData("PartitionId0",
                                                                    new List<string>(),
                                                                    1,
                                                                    12,
                                                                    50,
                                                                    2,
                                                                    new PodConfiguration(new Dictionary<string, string>())),
                                                  new PartitionData("PartitionId1",
                                                                    new List<string>(),
                                                                    1,
                                                                    10,
                                                                    50,
                                                                    1,
                                                                    new PodConfiguration(new Dictionary<string, string>())),
                                                  new PartitionData("PartitionId2",
                                                                    new List<string>
                                                                    {
                                                                      "ParentPartitionId",
                                                                    },
                                                                    1,
                                                                    13,
                                                                    50,
                                                                    1,
                                                                    new PodConfiguration(new Dictionary<string, string>())),
                                                })
                         .ConfigureAwait(false);
  }

  [TearDown]
  public virtual void TearDown()
  {
    PartitionTable = null;
    RunTests       = false;
  }

  private static bool CheckForSkipSetup()
  {
    var category = TestContext.CurrentContext.Test.Properties.Get("Category") as string;
    return category is "SkipSetUp";
  }

  /* Interface to test */
  protected IPartitionTable? PartitionTable;

  /* Boolean to control that tests are executed in
   * an instance of this class */
  protected bool RunTests;

  /* Function be override so it returns the suitable instance
   * of PartitionTable to the corresponding interface implementation */
  public virtual void GetPartitionTableInstance()
  {
  }

  private const string PartitionPath = $"{InitServices.SettingSection}:{Partitioning.SettingSection}:{nameof(Partitioning.Partitions)}";


  private static readonly Partition PartitionEnv = new()
                                                   {
                                                     Priority             = 2,
                                                     PodMax               = 2,
                                                     PodReserved          = 2,
                                                     PreemptionPercentage = 2,
                                                     PartitionId          = "Partition1",
                                                     ParentPartitionIds = new List<string>
                                                                          {
                                                                            "PartitionParent1",
                                                                          },
                                                     PodConfiguration = new Dictionary<string, string>
                                                                        {
                                                                          {
                                                                            "key1", "val1"
                                                                          },
                                                                          {
                                                                            "key2", "val2"
                                                                          },
                                                                        },
                                                   };

  [Test]
  [Category("SkipSetUp")]
  public async Task InitShouldSucceed()
  {
    if (RunTests)
    {
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PartitionTable!.Check(HealthCheckTag.Liveness)
                                               .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PartitionTable.Check(HealthCheckTag.Readiness)
                                              .ConfigureAwait(false)).Status);
      Assert.AreNotEqual(HealthStatus.Healthy,
                         (await PartitionTable.Check(HealthCheckTag.Startup)
                                              .ConfigureAwait(false)).Status);

      await PartitionTable.Init(CancellationToken.None)
                          .ConfigureAwait(false);

      Assert.AreEqual(HealthStatus.Healthy,
                      (await PartitionTable.Check(HealthCheckTag.Liveness)
                                           .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await PartitionTable.Check(HealthCheckTag.Readiness)
                                           .ConfigureAwait(false)).Status);
      Assert.AreEqual(HealthStatus.Healthy,
                      (await PartitionTable.Check(HealthCheckTag.Startup)
                                           .ConfigureAwait(false)).Status);
    }
  }

  [Test]
  [Category("DoSetup")]
  public async Task ReadPartitionAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = await PartitionTable!.ReadPartitionAsync("PartitionId0",
                                                            CancellationToken.None)
                                        .ConfigureAwait(false);

      Assert.AreEqual("PartitionId0",
                      result.PartitionId);
    }
  }

  [Test]
  public async Task ReadPartitionAsyncFromEnvShouldSucceed()
  {
    if (RunTests)
    {
      var result = await PartitionTable!.ReadPartitionAsync(PartitionEnv.PartitionId,
                                                            CancellationToken.None)
                                        .ConfigureAwait(false);

      Assert.AreEqual(PartitionEnv.PartitionId,
                      result.PartitionId);
      Assert.AreEqual(PartitionEnv.PodReserved,
                      result.PodReserved);
      Assert.AreEqual(PartitionEnv.PodConfiguration,
                      result.PodConfiguration!.Configuration);
    }
  }

  [Test]
  public Task ReadTaskAsyncShouldThrowException()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<PartitionNotFoundException>(() => PartitionTable!.ReadPartitionAsync("PartitionIdNotFound",
                                                                                              CancellationToken.None));
    }

    return Task.CompletedTask;
  }

  [Test]
  public Task DeleteTaskAsyncShouldThrowException()
  {
    if (RunTests)
    {
      Assert.ThrowsAsync<PartitionNotFoundException>(() => PartitionTable!.DeletePartitionAsync("PartitionIdNotFound",
                                                                                                CancellationToken.None));
    }

    return Task.CompletedTask;
  }

  [Test]
  public async Task DeleteAlreadyDeletedTaskAsyncShouldThrowException()
  {
    if (RunTests)
    {
      await PartitionTable!.DeletePartitionAsync("PartitionId0",
                                                 CancellationToken.None)
                           .ConfigureAwait(false);

      Assert.ThrowsAsync<PartitionNotFoundException>(() => PartitionTable!.DeletePartitionAsync("PartitionId0",
                                                                                                CancellationToken.None));
    }
  }

  [Test]
  public async Task ArePartitionExistingAsyncShouldSucceed()
  {
    if (RunTests)
    {
      var result = await PartitionTable!.ArePartitionsExistingAsync(new[]
                                                                    {
                                                                      "PartitionId0",
                                                                      "PartitionId1",
                                                                    },
                                                                    CancellationToken.None)
                                        .ConfigureAwait(false);

      Assert.IsTrue(result);
    }
  }

  [Test]
  public async Task ArePartitionExistingAsyncShouldReturnFalse()
  {
    if (RunTests)
    {
      var result = await PartitionTable!.ArePartitionsExistingAsync(new[]
                                                                    {
                                                                      "PartitionId0",
                                                                      "PartitionIdNotExisting",
                                                                    },
                                                                    CancellationToken.None)
                                        .ConfigureAwait(false);

      Assert.IsFalse(result);
    }
  }

  [Test]
  public async Task ListPartitionsEmptyResultShouldSucceed()
  {
    if (RunTests)
    {
      var (_, totalCount) = await PartitionTable!.ListPartitionsAsync(data => data.PartitionId == "NotExisting",
                                                                      data => data.ParentPartitionIds,
                                                                      false,
                                                                      0,
                                                                      20,
                                                                      CancellationToken.None)
                                                 .ConfigureAwait(false);

      Assert.AreEqual(0,
                      totalCount);
    }
  }

  [Test]
  public async Task ListPartitionsContainsShouldSucceed()
  {
    if (RunTests)
    {
      var (_, totalCount) = await PartitionTable!.ListPartitionsAsync(data => data.ParentPartitionIds.Contains("ParentPartitionId"),
                                                                      data => data.PartitionId,
                                                                      false,
                                                                      0,
                                                                      20,
                                                                      CancellationToken.None)
                                                 .ConfigureAwait(false);

      Assert.AreEqual(1,
                      totalCount);
    }
  }

  [Test]
  [TestCaseSource(nameof(TestCasesFilter))]
  public async Task ListPartitionFilter(ListPartitionsRequest request,
                                        int                   count)
  {
    if (RunTests)
    {
      var (_, totalCount) = await PartitionTable!.ListPartitionsAsync(request.Filters.ToPartitionFilter(),
                                                                      data => data.PartitionId,
                                                                      false,
                                                                      0,
                                                                      20,
                                                                      CancellationToken.None)
                                                 .ConfigureAwait(false);

      Assert.AreEqual(count,
                      totalCount);
    }
  }

  public static IEnumerable<TestCaseData> TestCasesFilter()
  {
    TestCaseData CaseTrue(FilterField filterField)
      => new TestCaseData(ListPartitionsHelper.CreateListPartitionsRequest(new ListPartitionsRequest.Types.Sort(),
                                                                           new[]
                                                                           {
                                                                             filterField,
                                                                             ListPartitionsHelper.CreateListPartitionsFilterString(PartitionRawEnumField.Id,
                                                                                                                                   FilterStringOperator.Equal,
                                                                                                                                   "PartitionId2"),
                                                                           }),
                          1).SetArgDisplayNames(filterField + " true");

    TestCaseData CaseFalse(FilterField filterField)
      => new TestCaseData(ListPartitionsHelper.CreateListPartitionsRequest(new ListPartitionsRequest.Types.Sort(),
                                                                           new[]
                                                                           {
                                                                             filterField,
                                                                             ListPartitionsHelper.CreateListPartitionsFilterString(PartitionRawEnumField.Id,
                                                                                                                                   FilterStringOperator.Equal,
                                                                                                                                   "PartitionId2"),
                                                                           }),
                          0).SetArgDisplayNames(filterField + " false");

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterString(PartitionRawEnumField.Id,
                                                                                FilterStringOperator.Equal,
                                                                                "PartitionId2"));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterString(PartitionRawEnumField.Id,
                                                                                 FilterStringOperator.Equal,
                                                                                 "PartitionId_false"));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodReserved,
                                                                                FilterNumberOperator.Equal,
                                                                                1));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodReserved,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodMax,
                                                                                FilterNumberOperator.Equal,
                                                                                13));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PodMax,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PreemptionPercentage,
                                                                                FilterNumberOperator.Equal,
                                                                                50));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.PreemptionPercentage,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.Priority,
                                                                                FilterNumberOperator.Equal,
                                                                                1));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterNumber(PartitionRawEnumField.Priority,
                                                                                 FilterNumberOperator.Equal,
                                                                                 2));

    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterArray(PartitionRawEnumField.ParentPartitionIds,
                                                                               FilterArrayOperator.Contains,
                                                                               "ParentPartitionId"));
    yield return CaseTrue(ListPartitionsHelper.CreateListPartitionsFilterArray(PartitionRawEnumField.ParentPartitionIds,
                                                                               FilterArrayOperator.NotContains,
                                                                               "AnotherParentPartitionId"));
    yield return CaseFalse(ListPartitionsHelper.CreateListPartitionsFilterArray(PartitionRawEnumField.ParentPartitionIds,
                                                                                FilterArrayOperator.Contains,
                                                                                "AnotherParentPartitionId"));
  }
}
