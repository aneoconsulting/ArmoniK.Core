
using ArmoniK.Core.gRPC.V1;
using FluentValidation;

namespace ArmoniK.Core.gRPC
{
    public class TaskOptionsValidator : AbstractValidator<TaskOptions>
    {
        public TaskOptionsValidator()
        {
            RuleFor(o => o.MaxRetries).GreaterThanOrEqualTo(1);
        }
    }
}
