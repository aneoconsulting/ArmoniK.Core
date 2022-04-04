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

using ArmoniK.Api.gRPC.V1;

using FluentValidation;

using JetBrains.Annotations;

namespace ArmoniK.Core.Common.gRPC.Validators;

[UsedImplicitly]
public class TaskFilterValidator : AbstractValidator<TaskFilter>
{
  public TaskFilterValidator()
  {
    RuleFor(filter => filter.IdsCase).NotEqual(TaskFilter.IdsOneofCase.None);
    RuleFor(filter => filter.Dispatch).NotNull()
                                      .NotEmpty()
                                      .When(filter => filter.IdsCase == TaskFilter.IdsOneofCase.Dispatch);
    RuleFor(filter => filter.Dispatch).Null().Empty()
                                              .When(filter => filter.IdsCase is TaskFilter.IdsOneofCase.Task or TaskFilter.IdsOneofCase.Session);
    RuleFor(filter => filter.Session).NotNull()
                                     .NotEmpty()
                                     .When(filter => filter.IdsCase == TaskFilter.IdsOneofCase.Session);
    RuleFor(filter => filter.Session).Null().Empty()
                                     .When(filter => filter.IdsCase is TaskFilter.IdsOneofCase.Dispatch or TaskFilter.IdsOneofCase.Task);
    RuleFor(filter => filter.Task).NotNull()
                                  .NotEmpty()
                                  .When(filter => filter.IdsCase == TaskFilter.IdsOneofCase.Task);
    RuleFor(filter => filter.Task).Null().Empty()
                                  .When(filter => filter.IdsCase is TaskFilter.IdsOneofCase.Dispatch or TaskFilter.IdsOneofCase.Session);
  }
}