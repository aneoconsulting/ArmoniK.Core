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

using ArmoniK.Api.gRPC.V1.Applications;

using FluentValidation;

namespace ArmoniK.Core.Common.gRPC.Validators;

/// <summary>
///   Validator for <see cref="ListApplicationsRequest" />
/// </summary>
public class ListApplicationsRequestValidator : AbstractValidator<ListApplicationsRequest>
{
  /// <summary>
  ///   Initializes a validator for <see cref="ListApplicationsRequest" />
  /// </summary>
  public ListApplicationsRequestValidator()
  {
    RuleFor(request => request.Page)
      .NotNull()
      .GreaterThanOrEqualTo(0)
      .WithName($"{nameof(ListApplicationsRequest)}.{nameof(ListApplicationsRequest.Page)}");
    RuleFor(request => request.PageSize)
      .NotNull()
      .GreaterThanOrEqualTo(1)
      .WithName($"{nameof(ListApplicationsRequest)}.{nameof(ListApplicationsRequest.PageSize)}");
    RuleFor(request => request.Filters)
      .NotNull()
      .NotEmpty()
      .WithName($"{nameof(ListApplicationsRequest)}.{nameof(ListApplicationsRequest.Filters)}");
    RuleFor(request => request.Sort)
      .NotNull()
      .NotEmpty()
      .WithName($"{nameof(ListApplicationsRequest)}.{nameof(ListApplicationsRequest.Sort)}")
      .DependentRules(() =>
                      {
                        RuleFor(request => request.Sort.Direction)
                          .NotNull()
                          .NotEmpty()
                          .WithName($"{nameof(ListApplicationsRequest)}.{nameof(ListApplicationsRequest.Sort)}.{nameof(ListApplicationsRequest.Sort.Direction)}");
                        RuleFor(request => request.Sort.Fields)
                          .NotNull()
                          .NotEmpty()
                          .WithName($"{nameof(ListApplicationsRequest)}.{nameof(ListApplicationsRequest.Sort)}.{nameof(ListApplicationsRequest.Sort.Fields)}");
                      });
  }
}
