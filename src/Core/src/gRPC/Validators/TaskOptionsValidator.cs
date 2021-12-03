using ArmoniK.Core.gRPC.V1;

using FluentValidation;

namespace ArmoniK.Core.gRPC.Validators
{
  public class TaskOptionsValidator : AbstractValidator<TaskOptions>
  {
    public TaskOptionsValidator()
    {
      RuleFor(o => o.MaxRetries).GreaterThanOrEqualTo(1);
      RuleFor(o => o.MaxDuration).NotNull();
      RuleFor(o => o.Priority).NotNull().GreaterThanOrEqualTo(0);
    }
  }
}