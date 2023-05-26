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
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Applications;
using ArmoniK.Core.Common.Storage;

using LinqKit;

namespace ArmoniK.Core.Common.gRPC;

public static class ListApplicationsRequestExt
{
  public static Expression<Func<Application, object?>> ToApplicationField(this ApplicationField field)
  {
    switch (field.FieldCase)
    {
      case ApplicationField.FieldOneofCase.ApplicationField_:
        return field.ApplicationField_ switch
               {
                 ApplicationRawField.Name        => taskData => taskData.Name,
                 ApplicationRawField.Version     => taskData => taskData.Version,
                 ApplicationRawField.Namespace   => taskData => taskData.Namespace,
                 ApplicationRawField.Service     => taskData => taskData.Service,
                 ApplicationRawField.Unspecified => throw new ArgumentOutOfRangeException(),
                 _                               => throw new ArgumentOutOfRangeException(),
               };

      case ApplicationField.FieldOneofCase.None:
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
