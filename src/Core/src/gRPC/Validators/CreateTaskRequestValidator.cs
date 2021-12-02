using ArmoniK.Core.gRPC.V1;
using FluentValidation;

namespace ArmoniK.Core.gRPC.Validators
{
    public class CreateTaskRequestValidator : AbstractValidator<CreateTaskRequest>
    {
        public CreateTaskRequestValidator()
        {
            RuleFor(request => request.TaskRequests).NotNull();
            RuleFor(request => request.TaskRequests).NotEmpty();
            RuleForEach(request => request.TaskRequests).SetValidator(new TaskRequestValidator());
        }
    }
}
