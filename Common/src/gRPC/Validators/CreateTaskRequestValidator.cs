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

using ArmoniK.Api.gRPC.V1;

using FluentValidation;

namespace ArmoniK.Core.Common.gRPC.Validators;

public class CreateSmallTaskRequestValidator : AbstractValidator<CreateSmallTaskRequest>
{
  public CreateSmallTaskRequestValidator()
  {
    RuleFor(r => r.SessionId).NotEmpty();
    RuleFor(r => r.TaskOptions).SetValidator(new TaskOptionsValidator());
    RuleFor(request => request.TaskRequests).NotEmpty();
    RuleForEach(request => request.TaskRequests).NotEmpty().SetValidator(new TaskRequestValidator());
  }


  public class TaskRequestValidator : AbstractValidator<CreateSmallTaskRequest.Types.TaskRequest>
  {
    public TaskRequestValidator()
    {
      RuleFor(r => r.DataDependencies).NotNull();
      RuleFor(r => r.ExpectedOutputKeys).NotNull();
      RuleFor(r => r.Payload).NotNull()
                             .Must(s => s.Length is > 0 and < PayloadConfiguration.MaxChunkSize)
                             .WithName(nameof(CreateSmallTaskRequest.Types.TaskRequest.Payload));
    }
  }
}


public class CreateLargeTaskRequestValidator : AbstractValidator<CreateLargeTaskRequest>
{
  public CreateLargeTaskRequestValidator()
  {
    RuleFor(r => r.RequestTypeCase).Must(oneOfCase => oneOfCase != CreateLargeTaskRequest.RequestTypeOneofCase.None);
    RuleFor(r => r.InitRequest).NotNull()
                               .SetValidator(new CreateLargeTaskInitRequestValidator())
                               .When(r => r.RequestTypeCase == CreateLargeTaskRequest.RequestTypeOneofCase.InitRequest);
    RuleFor(r => r.InitTask).NotNull()
                            .SetValidator(new CreateLargeTaskInitTaskValidator())
                            .When(r => r.RequestTypeCase == CreateLargeTaskRequest.RequestTypeOneofCase.InitTask);
  }


  private class CreateLargeTaskInitRequestValidator : AbstractValidator<CreateLargeTaskRequest.Types.InitRequest>
  {
    public CreateLargeTaskInitRequestValidator()
    {
      RuleFor(r => r.SessionId).NotEmpty();
      RuleFor(r => r.TaskOptions).SetValidator(new TaskOptionsValidator());
    }
  }

  private class CreateLargeTaskInitTaskValidator : AbstractValidator<CreateLargeTaskRequest.Types.InitTaskRequest>
  {
    public CreateLargeTaskInitTaskValidator()
    {
      RuleFor(r => r.DataDependencies).NotNull();
      RuleFor(r => r.ExpectedOutputKeys).NotNull();
      RuleFor(r => r.Id).NotNull().NotEmpty();
      RuleFor(r => r.PayloadChunk).NotNull()
                             .Must(s => s.Length is > 0 and < PayloadConfiguration.MaxChunkSize);
    }
  }

  private class CreateLargeTaskPayloadValidator : AbstractValidator<CreateLargeTaskRequest.Types.PayloadRequest>
  {
    public CreateLargeTaskPayloadValidator()
    {
      RuleFor(r => r.PayloadChunk).NotNull()
                             .Must(s => s.Length is > 0 and < PayloadConfiguration.MaxChunkSize);
    }
  }


}