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

namespace ArmoniK.Core.Common.gRPC.Validators;

public class CreateSmallTaskRequestValidator : AbstractValidator<CreateSmallTaskRequest>
{
  public CreateSmallTaskRequestValidator()
  {
    RuleFor(r => r.SessionId).NotEmpty();
    RuleFor(r => r.TaskOptions).SetValidator(new TaskOptionsValidator()).NotNull();
    RuleFor(request => request.TaskRequests).NotEmpty();
    RuleForEach(request => request.TaskRequests).NotEmpty().SetValidator(new TaskRequestValidator());
  }


  public class TaskRequestValidator : AbstractValidator<TaskRequest>
  {
    public TaskRequestValidator()
    {
      RuleFor(r => r.DataDependencies).NotNull();
      RuleFor(r => r.ExpectedOutputKeys).NotNull();
      RuleFor(r => r.Payload).NotNull()
                             .Must(s => s.Length is > 0 and < PayloadConfiguration.MaxChunkSize)
                             .WithName(nameof(TaskRequest.Payload));
    }
  }
}


public class CreateLargeTaskRequestValidator : AbstractValidator<CreateLargeTaskRequest>
{
  public CreateLargeTaskRequestValidator()
  {
    RuleFor(r => r.TypeCase).NotEqual(CreateLargeTaskRequest.TypeOneofCase.None);
    RuleFor(r => r.InitRequest).NotNull()
                               .SetValidator(new CreateLargeTaskInitRequestValidator())
                               .When(r => r.TypeCase == CreateLargeTaskRequest.TypeOneofCase.InitRequest);
    RuleFor(r => r.InitTask).NotNull()
                            .SetValidator(new CreateLargeTaskInitTaskValidator())
                            .When(r => r.TypeCase == CreateLargeTaskRequest.TypeOneofCase.InitTask);
    RuleFor(r => r.TaskPayload).NotNull()
                            .SetValidator(new DataChunkValidator())
                            .When(r => r.TypeCase == CreateLargeTaskRequest.TypeOneofCase.TaskPayload);
  }


  private class CreateLargeTaskInitRequestValidator : AbstractValidator<CreateLargeTaskRequest.Types.InitRequest>
  {
    public CreateLargeTaskInitRequestValidator()
    {
      RuleFor(r => r.SessionId).NotEmpty();
      RuleFor(r => r.TaskOptions).SetValidator(new TaskOptionsValidator());
    }
  }

  private class CreateLargeTaskInitTaskValidator : AbstractValidator<InitTaskRequest>
  {
    public CreateLargeTaskInitTaskValidator()
    {
      RuleFor(r => r.TypeCase).NotEqual(InitTaskRequest.TypeOneofCase.None);
      RuleFor(r => r.Header).NotNull().
                             SetValidator(new CreateLargeTaskInitTaskHeaderValidator())
                            .When(r => r.TypeCase == InitTaskRequest.TypeOneofCase.Header);
      RuleFor(r => r.LastTask).Equal(true)
                              .When(r=>r.TypeCase == InitTaskRequest.TypeOneofCase.LastTask);
    }
  }

  private class CreateLargeTaskInitTaskHeaderValidator : AbstractValidator<TaskRequestHeader>
  {
    public CreateLargeTaskInitTaskHeaderValidator()
    {
      RuleFor(r => r.Id).NotNull().NotEmpty();
      RuleFor(r => r.ExpectedOutputKeys).NotNull().NotEmpty();
      RuleFor(r => r.DataDependencies).NotNull();
    }
  }

  private class DataChunkValidator : AbstractValidator<DataChunk>
  {
    public DataChunkValidator()
    {
      RuleFor(r => r.TypeCase).NotEqual(DataChunk.TypeOneofCase.None);
      RuleFor(r => r.Data).NotNull()
                          .Must(s => s.Length <= PayloadConfiguration.MaxChunkSize)
                          .When(r => r.TypeCase == DataChunk.TypeOneofCase.Data);
      RuleFor(r => r.DataComplete).Equal(true)
                                  .When(r => r.TypeCase == DataChunk.TypeOneofCase.DataComplete);
    }
  }


}