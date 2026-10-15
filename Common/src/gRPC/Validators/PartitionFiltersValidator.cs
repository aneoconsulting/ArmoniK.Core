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

using ArmoniK.Api.gRPC.V1.Partitions;

using FluentValidation;

namespace ArmoniK.Core.Common.gRPC.Validators;

/// <summary>
///   Validator for partition <see cref="Filters" />
/// </summary>
/// <remarks>
///   A condition must name the field it filters on and the condition on its value: the conversion of the filter
///   into a query cannot be made without them
/// </remarks>
public class PartitionFiltersValidator : AbstractValidator<Filters>
{
  /// <summary>
  ///   Initializes a validator for partition <see cref="Filters" />
  /// </summary>
  public PartitionFiltersValidator()
    => RuleForEach(filters => filters.Or)
      .ChildRules(branch => branch.RuleForEach(filtersAnd => filtersAnd.And)
                                  .ChildRules(condition =>
                                              {
                                                condition.RuleFor(filterField => filterField.Field)
                                                         .Must(field => field is not null && field.FieldCase != PartitionField.FieldOneofCase.None)
                                                         .WithMessage("A filter condition must name the field it filters on");
                                                condition.RuleFor(filterField => filterField.ValueConditionCase)
                                                         .NotEqual(FilterField.ValueConditionOneofCase.None)
                                                         .WithMessage("A filter condition must have a condition on the value of its field");
                                              }));
}
