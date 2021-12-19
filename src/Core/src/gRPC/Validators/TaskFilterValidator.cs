// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2021. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
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

using System.Linq;

using ArmoniK.Core.gRPC.V1;

using FluentValidation;

using JetBrains.Annotations;

namespace ArmoniK.Core.gRPC.Validators
{
  [UsedImplicitly]
  public class TaskFilterValidator : AbstractValidator<TaskFilter>
  {

    public TaskFilterValidator()
    {
      RuleFor(tf => tf).Must(filter => filter.IncludedTaskIds.All(id => !filter.ExcludedTaskIds.Contains(id)))
                       .WithMessage($"Content of {nameof(TaskFilter.IncludedTaskIds)} and {nameof(TaskFilter.ExcludedTaskIds)} must not overlap.");

      RuleFor(tf => tf).Must(filter => filter.IncludedStatuses.All(status => !filter.ExcludedStatuses.Contains(status)))
                       .WithMessage($"Content of {nameof(TaskFilter.IncludedStatuses)} and {nameof(TaskFilter.ExcludedStatuses)} must not overlap.");
    }
  }
}
