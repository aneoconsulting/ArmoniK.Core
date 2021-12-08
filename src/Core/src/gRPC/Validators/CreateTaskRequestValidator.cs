using ArmoniK.Core.gRPC.V1;

using FluentValidation;

using JetBrains.Annotations;

namespace ArmoniK.Core.gRPC.Validators
{
  [UsedImplicitly]
  public class CreateTaskRequestValidator : AbstractValidator<CreateTaskRequest>
  {
    public CreateTaskRequestValidator()
    {
      RuleFor(r => r.SessionId).NotNull().SetValidator(new SessionIdValidator());
      RuleFor(r => r.TaskOptions).SetValidator(new TaskOptionsValidator());
      RuleFor(request => request.TaskRequests).NotNull().NotEmpty();
      RuleForEach(request => request.TaskRequests).SetValidator(new TaskRequestValidator());
    }
  }
}