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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.gRPC.Convertors;

using Google.Protobuf.Collections;

using NUnit.Framework;

namespace ArmoniK.Core.Common.Tests;

[TestFixture(TestOf = typeof(TaskOptions))]
public class TaskOptionsTests
{
  private readonly TaskOptions? options_ = null;

  private readonly Base.DataStructures.TaskOptions completeOptions_ = new(new MapField<string, string>
                                                                          {
                                                                            {
                                                                              "key1", "val1"
                                                                            },
                                                                            {
                                                                              "key2", "val2"
                                                                            },
                                                                          },
                                                                          TimeSpan.FromSeconds(1),
                                                                          5,
                                                                          1,
                                                                          "PartitionId",
                                                                          "ApplicationName",
                                                                          "ApplicationVersion",
                                                                          "ApplicationNamespace",
                                                                          "ApplicationService",
                                                                          "EngineType");


  [Test]
  public void NullTaskOptionsShouldBeEqual()
    => Assert.That(options_.ToNullableTaskOptions(),
                   Is.SameAs(null));

  [Test]
  public void ConversionShouldBeEqual()
    => Assert.That(completeOptions_.ToGrpcTaskOptions()
                                   .ToTaskOptions(),
                   Is.EqualTo(completeOptions_));

  [Test]
  public void MergeWithNullTaskOptionShouldReturnDefault()
    => Assert.That(Base.DataStructures.TaskOptions.Merge(null,
                                                         completeOptions_),
                   Is.EqualTo(completeOptions_));

  [Test]
  public void MergeShouldKeepSetValuesFromTaskOption()
  {
    var defaultOptions = new Base.DataStructures.TaskOptions(new MapField<string, string>(),
                                                             TimeSpan.FromSeconds(10),
                                                             99,
                                                             42,
                                                             "DefaultPartition",
                                                             "DefaultName",
                                                             "DefaultVersion",
                                                             "DefaultNamespace",
                                                             "DefaultService",
                                                             "DefaultEngine");

    var merged = Base.DataStructures.TaskOptions.Merge(completeOptions_,
                                                       defaultOptions);

    Assert.Multiple(() =>
                    {
                      Assert.That(merged.MaxDuration,
                                  Is.EqualTo(completeOptions_.MaxDuration));
                      Assert.That(merged.MaxRetries,
                                  Is.EqualTo(completeOptions_.MaxRetries));
                      Assert.That(merged.Priority,
                                  Is.EqualTo(completeOptions_.Priority));
                      Assert.That(merged.PartitionId,
                                  Is.EqualTo(completeOptions_.PartitionId));
                      Assert.That(merged.ApplicationName,
                                  Is.EqualTo(completeOptions_.ApplicationName));
                      Assert.That(merged.ApplicationVersion,
                                  Is.EqualTo(completeOptions_.ApplicationVersion));
                      Assert.That(merged.ApplicationNamespace,
                                  Is.EqualTo(completeOptions_.ApplicationNamespace));
                      Assert.That(merged.ApplicationService,
                                  Is.EqualTo(completeOptions_.ApplicationService));
                      Assert.That(merged.EngineType,
                                  Is.EqualTo(completeOptions_.EngineType));
                    });
  }

  [Test]
  public void MergeShouldFallBackToDefaultForUnsetValues()
  {
    var emptyOptions = new Base.DataStructures.TaskOptions();

    var merged = Base.DataStructures.TaskOptions.Merge(emptyOptions,
                                                       completeOptions_);

    Assert.Multiple(() =>
                    {
                      Assert.That(merged.MaxDuration,
                                  Is.EqualTo(completeOptions_.MaxDuration));
                      Assert.That(merged.MaxRetries,
                                  Is.EqualTo(completeOptions_.MaxRetries));
                      Assert.That(merged.PartitionId,
                                  Is.EqualTo(completeOptions_.PartitionId));
                      Assert.That(merged.ApplicationName,
                                  Is.EqualTo(completeOptions_.ApplicationName));
                      Assert.That(merged.ApplicationVersion,
                                  Is.EqualTo(completeOptions_.ApplicationVersion));
                      Assert.That(merged.ApplicationNamespace,
                                  Is.EqualTo(completeOptions_.ApplicationNamespace));
                      Assert.That(merged.ApplicationService,
                                  Is.EqualTo(completeOptions_.ApplicationService));
                      Assert.That(merged.EngineType,
                                  Is.EqualTo(completeOptions_.EngineType));
                    });
  }

  [Test]
  public void MergeShouldOverlayOptionsDictionary()
  {
    var defaultOptions = new Base.DataStructures.TaskOptions(new MapField<string, string>
                                                             {
                                                               {
                                                                 "key1", "default1"
                                                               },
                                                               {
                                                                 "key3", "default3"
                                                               },
                                                             },
                                                             TimeSpan.Zero,
                                                             0,
                                                             0,
                                                             "",
                                                             "",
                                                             "",
                                                             "",
                                                             "",
                                                             "");

    var merged = Base.DataStructures.TaskOptions.Merge(completeOptions_,
                                                       defaultOptions);

    Assert.Multiple(() =>
                    {
                      // Overridden by taskOption
                      Assert.That(merged.Options["key1"],
                                  Is.EqualTo("val1"));
                      // Only in taskOption
                      Assert.That(merged.Options["key2"],
                                  Is.EqualTo("val2"));
                      // Only in default, preserved
                      Assert.That(merged.Options["key3"],
                                  Is.EqualTo("default3"));
                    });
  }
}
