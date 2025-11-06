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

using ArmoniK.Api.gRPC.V1;

using FluentValidation;

namespace ArmoniK.Core.Common.gRPC.Validators;

/// <summary>
///   Validator for <see cref="TaskOptions" />
/// </summary>
public class TaskOptionsValidator : AbstractValidator<TaskOptions>
{
  /// <summary>
  ///   Initializes a validator for <see cref="TaskOptions" />
  /// </summary>
  public TaskOptionsValidator(bool isSession = false)
  {
    var minRetries = isSession
                       ? 1
                       : 0;
    var minPriority = isSession
                        ? 1
                        : 0;
    RuleFor(o => o.MaxRetries)
      .GreaterThanOrEqualTo(minRetries)
      .WithMessage($"MaxRetries should be greater or equal than {minRetries}")
      .WithName(nameof(TaskOptions.MaxRetries));
    RuleFor(o => o.Priority)
      .GreaterThanOrEqualTo(minPriority)
      .LessThanOrEqualTo(99)
      .WithMessage($"Priority should be included between {minPriority} and 99")
      .WithName(nameof(TaskOptions.Priority));
    if (isSession)
    {
      RuleFor(o => o.MaxDuration)
        .NotNull()
        .NotEmpty()
        .WithMessage("MaxDuration is required")
        .WithName(nameof(TaskOptions.MaxDuration));
    }
  }
}
