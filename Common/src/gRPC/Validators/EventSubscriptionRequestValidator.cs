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

using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Api.gRPC.V1.Tasks;

using FluentValidation;
using FluentValidation.Results;

namespace ArmoniK.Core.Common.gRPC.Validators;

/// <summary>
///   Validator for <see cref="EventSubscriptionRequest" />
/// </summary>
public class EventSubscriptionRequestValidator : AbstractValidator<EventSubscriptionRequest>
{
  /// <inheritdoc />
  public override ValidationResult Validate(ValidationContext<EventSubscriptionRequest> context)
  {
    if (context.InstanceToValidate.ResultsFilters != null)
    {
      foreach (var filtersAnd in context.InstanceToValidate.ResultsFilters.Or)
      {
        foreach (var filterField in filtersAnd.And)
        {
          if (filterField?.Field is null)
          {
            context.AddFailure("Field should not be null");
            continue;
          }

          switch (filterField.Field.FieldCase)
          {
            case ResultField.FieldOneofCase.ResultRawField:
              switch (filterField.Field.ResultRawField.Field)
              {
                case ResultRawEnumField.OwnerTaskId:
                case ResultRawEnumField.SessionId:
                case ResultRawEnumField.Name:
                case ResultRawEnumField.ResultId:
                  break;
                case ResultRawEnumField.Status:
                case ResultRawEnumField.CreatedAt:
                case ResultRawEnumField.CompletedAt:
                case ResultRawEnumField.Unspecified:
                default:
                  context.AddFailure($"Cannot filter {filterField.Field.ResultRawField.Field} on in this API");
                  break;
              }

              break;
            case ResultField.FieldOneofCase.None:
            default:
              context.AddFailure($"Cannot filter {filterField.Field} on in this API");
              break;
          }
        }
      }
    }

    // ReSharper disable once InvertIf
    if (context.InstanceToValidate.TasksFilters != null)
    {
      foreach (var filtersAnd in context.InstanceToValidate.TasksFilters.Or)
      {
        foreach (var filterField in filtersAnd.And)
        {
          if (filterField?.Field is null)
          {
            context.AddFailure("Field should not be null");
            continue;
          }

          switch (filterField.Field.FieldCase)
          {
            case TaskField.FieldOneofCase.TaskSummaryField:
              switch (filterField.Field.TaskSummaryField.Field)
              {
                case TaskSummaryEnumField.TaskId:
                case TaskSummaryEnumField.SessionId:
                case TaskSummaryEnumField.OwnerPodId:
                case TaskSummaryEnumField.InitialTaskId:
                case TaskSummaryEnumField.PodHostname:
                  break;

                case TaskSummaryEnumField.Unspecified:
                case TaskSummaryEnumField.Status:
                case TaskSummaryEnumField.ReceivedAt:
                case TaskSummaryEnumField.AcquiredAt:
                case TaskSummaryEnumField.CreatedAt:
                case TaskSummaryEnumField.SubmittedAt:
                case TaskSummaryEnumField.StartedAt:
                case TaskSummaryEnumField.EndedAt:
                case TaskSummaryEnumField.CreationToEndDuration:
                case TaskSummaryEnumField.ProcessingToEndDuration:
                case TaskSummaryEnumField.Error:
                case TaskSummaryEnumField.PodTtl:
                default:
                  context.AddFailure($"Cannot filter {filterField.Field.TaskSummaryField.Field} on in this API");
                  break;
              }

              break;
            case TaskField.FieldOneofCase.TaskOptionField:
            case TaskField.FieldOneofCase.TaskOptionGenericField:
              break;
            case TaskField.FieldOneofCase.None:
            default:
              context.AddFailure($"Cannot filter {filterField.Field} on in this API");
              break;
          }
        }
      }
    }

    return base.Validate(context);
  }
}
