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
using System.Linq.Expressions;

namespace ArmoniK.Core.Common.Utils;

public static class IQueryableExt
{
  public static IOrderedQueryable<T> OrderByList<T>(this IQueryable<T>                        queryable,
                                                    ICollection<Expression<Func<T, object?>>> orderFields,
                                                    bool                                      ascOrder = true)
  {
    if (ascOrder)
    {
      var ordered = queryable.OrderBy(orderFields.First());
      if (orderFields.Count > 1)
      {
        foreach (var expression in orderFields.Skip(1))
        {
          ordered = ordered.ThenBy(expression);
        }
      }

      return ordered;
    }
    else
    {
      var ordered = queryable.OrderByDescending(orderFields.First());
      if (orderFields.Count > 1)
      {
        foreach (var expression in orderFields.Skip(1))
        {
          ordered = ordered.ThenByDescending(expression);
        }
      }

      return ordered;
    }
  }
}
