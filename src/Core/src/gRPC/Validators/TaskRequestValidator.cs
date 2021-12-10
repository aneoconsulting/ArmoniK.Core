using ArmoniK.Core.gRPC.V1;

using FluentValidation;

namespace ArmoniK.Core.gRPC.Validators
{
  public class TaskRequestValidator : AbstractValidator<TaskRequest>
  {
    public TaskRequestValidator()
    {
      RuleFor(r => r.Payload).NotNull().SetValidator(new PayloadValidator());
    }
  }
}