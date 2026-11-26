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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Core.Common.Exceptions;
using ArmoniK.Core.Common.gRPC;

using NUnit.Framework;

using Applications = ArmoniK.Api.gRPC.V1.Applications;
using Partitions = ArmoniK.Api.gRPC.V1.Partitions;
using Results = ArmoniK.Api.gRPC.V1.Results;
using Sessions = ArmoniK.Api.gRPC.V1.Sessions;
using Tasks = ArmoniK.Api.gRPC.V1.Tasks;

namespace ArmoniK.Core.Common.Tests.Grpc;

/// <summary>
///   A filter or a sort that cannot be converted into a query is an error of the request
/// </summary>
[TestFixture]
public class InvalidFilterTests
{
  private static readonly FilterString AnyString = new()
                                                   {
                                                     Operator = FilterStringOperator.Equal,
                                                     Value    = "x",
                                                   };

  [Test]
  public void ASessionConditionWithoutFieldIsInvalid()
    => Assert.That(() => new Sessions.Filters
                         {
                           Or =
                           {
                             new Sessions.FiltersAnd
                             {
                               And =
                               {
                                 new Sessions.FilterField
                                 {
                                   FilterString = AnyString,
                                 },
                               },
                             },
                           },
                         }.ToSessionDataFilter(),
                   Throws.InstanceOf<InvalidFilterException>());

  [Test]
  public void AnUnspecifiedSessionFieldIsInvalid()
    => Assert.That(() => new Sessions.Filters
                         {
                           Or =
                           {
                             new Sessions.FiltersAnd
                             {
                               And =
                               {
                                 new Sessions.FilterField
                                 {
                                   Field = new Sessions.SessionField
                                           {
                                             SessionRawField = new Sessions.SessionRawField
                                                               {
                                                                 Field = Sessions.SessionRawEnumField.Unspecified,
                                                               },
                                           },
                                   FilterString = AnyString,
                                 },
                               },
                             },
                           },
                         }.ToSessionDataFilter(),
                   Throws.InstanceOf<InvalidFilterException>());

  [Test]
  public void ANumberConditionOnAStringTaskFieldIsInvalid()
    => Assert.That(() => new Tasks.Filters
                         {
                           Or =
                           {
                             new Tasks.FiltersAnd
                             {
                               And =
                               {
                                 new Tasks.FilterField
                                 {
                                   Field = new Tasks.TaskField
                                           {
                                             TaskSummaryField = new Tasks.TaskSummaryField
                                                                {
                                                                  Field = Tasks.TaskSummaryEnumField.TaskId,
                                                                },
                                           },
                                   FilterNumber = new FilterNumber
                                                  {
                                                    Operator = FilterNumberOperator.Equal,
                                                    Value    = 1,
                                                  },
                                 },
                               },
                             },
                           },
                         }.ToTaskDataFilter(),
                   Throws.InstanceOf<InvalidFilterException>());

  [Test]
  public void AResultConditionWithoutFieldIsInvalid()
    => Assert.That(() => new Results.Filters
                         {
                           Or =
                           {
                             new Results.FiltersAnd
                             {
                               And =
                               {
                                 new Results.FilterField
                                 {
                                   FilterString = AnyString,
                                 },
                               },
                             },
                           },
                         }.ToResultFilter(),
                   Throws.InstanceOf<InvalidFilterException>());

  [Test]
  public void APartitionConditionWithoutFieldIsInvalid()
    => Assert.That(() => new Partitions.Filters
                         {
                           Or =
                           {
                             new Partitions.FiltersAnd
                             {
                               And =
                               {
                                 new Partitions.FilterField
                                 {
                                   FilterString = AnyString,
                                 },
                               },
                             },
                           },
                         }.ToPartitionFilter(),
                   Throws.InstanceOf<InvalidFilterException>());

  [Test]
  public void AnApplicationConditionWithoutFieldIsInvalid()
    => Assert.That(() => new Applications.Filters
                         {
                           Or =
                           {
                             new Applications.FiltersAnd
                             {
                               And =
                               {
                                 new Applications.FilterField
                                 {
                                   FilterString = AnyString,
                                 },
                               },
                             },
                           },
                         }.ToApplicationFilter(),
                   Throws.InstanceOf<InvalidFilterException>());

  [Test]
  public void ASortWithoutFieldIsInvalid()
    => Assert.Multiple(() =>
                       {
                         Assert.That(() => new Sessions.ListSessionsRequest.Types.Sort().ToField(),
                                     Throws.InstanceOf<InvalidFilterException>());
                         Assert.That(() => new Tasks.ListTasksRequest.Types.Sort().ToField(),
                                     Throws.InstanceOf<InvalidFilterException>());
                         Assert.That(() => new Results.ListResultsRequest.Types.Sort().ToField(),
                                     Throws.InstanceOf<InvalidFilterException>());
                         Assert.That(() => new Partitions.ListPartitionsRequest.Types.Sort().ToField(),
                                     Throws.InstanceOf<InvalidFilterException>());
                         Assert.That(() => new Applications.ApplicationField().ToField(),
                                     Throws.InstanceOf<InvalidFilterException>());
                       });

  [Test]
  public void AValidFilterIsConverted()
    => Assert.That(() => new Sessions.Filters
                         {
                           Or =
                           {
                             new Sessions.FiltersAnd
                             {
                               And =
                               {
                                 new Sessions.FilterField
                                 {
                                   Field = new Sessions.SessionField
                                           {
                                             SessionRawField = new Sessions.SessionRawField
                                                               {
                                                                 Field = Sessions.SessionRawEnumField.SessionId,
                                                               },
                                           },
                                   FilterString = AnyString,
                                 },
                               },
                             },
                           },
                         }.ToSessionDataFilter(),
                   Throws.Nothing);
}
