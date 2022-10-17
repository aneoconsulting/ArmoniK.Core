// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
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
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using ArmoniK.Api.gRPC.V1.Results;

using FluentValidation;

namespace ArmoniK.Core.Common.gRPC.Validators;

/// <summary>
///   Validator for <see cref="ListResultsRequest" />
/// </summary>
public class ListResultsRequestValidator : AbstractValidator<ListResultsRequest>
{
  /// <summary>
  ///   Initializes a validator for <see cref="ListResultsRequest" />
  /// </summary>
  public ListResultsRequestValidator()
  {
    RuleFor(request => request.Page)
      .NotNull()
      .GreaterThanOrEqualTo(0)
      .WithName($"{nameof(ListResultsRequest)}.{nameof(ListResultsRequest.Page)}");
    RuleFor(request => request.PageSize)
      .NotNull()
      .GreaterThanOrEqualTo(0)
      .WithName($"{nameof(ListResultsRequest)}.{nameof(ListResultsRequest.PageSize)}");
    RuleFor(request => request.Filter)
      .NotNull()
      .NotEmpty()
      .WithName($"{nameof(ListResultsRequest)}.{nameof(ListResultsRequest.Filter)}");
    RuleFor(request => request.Sort)
      .NotNull()
      .NotEmpty()
      .WithName($"{nameof(ListResultsRequest)}.{nameof(ListResultsRequest.Sort)}")
      .DependentRules(() =>
                      {
                        RuleFor(request => request.Sort.Order)
                          .NotNull()
                          .NotEmpty()
                          .WithName($"{nameof(ListResultsRequest)}.{nameof(ListResultsRequest.Sort)}.{nameof(ListResultsRequest.Sort.Order)}");
                        RuleFor(request => request.Sort.Field)
                          .NotNull()
                          .NotEmpty()
                          .WithName($"{nameof(ListResultsRequest)}.{nameof(ListResultsRequest.Sort)}.{nameof(ListResultsRequest.Sort.Field)}");
                      });
  }
}
