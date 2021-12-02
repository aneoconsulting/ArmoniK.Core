using ArmoniK.Core.gRPC.V1;
using FluentValidation;

namespace ArmoniK.Core.gRPC.Validators
{
    public class TaskIdValidator : AbstractValidator<TaskId>
    {
        public TaskIdValidator()
        {
            RuleFor(o => o.Session).NotNull();
            RuleFor(o => o.Task).NotNull();
        }
    }
}
