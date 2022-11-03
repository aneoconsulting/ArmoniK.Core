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
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListApplicationsRequestExt
{
  public static Expression<Func<Application, object?>> ToApplicationField(this ListApplicationsRequest.Types.Sort sort)
  {
    switch (sort.Field)
    {
      case ListApplicationsRequest.Types.OrderByField.Name:
        return taskData => taskData.Name;

      case ListApplicationsRequest.Types.OrderByField.Version:
        return taskData => taskData.Version;

      case ListApplicationsRequest.Types.OrderByField.Namespace:
        return taskData => taskData.Namespace;

      case ListApplicationsRequest.Types.OrderByField.Service:
        return taskData => taskData.Service;

      case ListApplicationsRequest.Types.OrderByField.Unspecified:
      default:
        throw new ArgumentOutOfRangeException();
    }
  }

  public static Expression<Func<TaskData, bool>> ToApplicationFilter(this ListApplicationsRequest.Types.Filter filter)
  {
    var predicate = PredicateBuilder.New<TaskData>();
    predicate = predicate.And(data => true);

    if (!string.IsNullOrEmpty(filter.Namespace))
    {
      predicate = predicate.And(data => data.Options.ApplicationNamespace == filter.Namespace);
    }

    if (!string.IsNullOrEmpty(filter.Name))
    {
      predicate = predicate.And(data => data.Options.ApplicationName == filter.Name);
    }

    if (!string.IsNullOrEmpty(filter.Service))
    {
      predicate = predicate.And(data => data.Options.ApplicationService == filter.Service);
    }

    if (!string.IsNullOrEmpty(filter.Version))
    {
      predicate = predicate.And(data => data.Options.ApplicationVersion == filter.Version);
    }

    return predicate;
  }
}
