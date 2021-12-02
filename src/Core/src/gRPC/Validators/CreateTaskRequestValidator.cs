
using ArmoniK.Core.gRPC.V1;
using FluentValidation;

namespace ArmoniK.Core.gRPC
{
    public class CreateTaskRequestValidator : AbstractValidator<CreateTaskRequest>
    {
        public CreateTaskRequestValidator()
        {
            RuleFor(request => request.TaskRequests).NotNull();
            RuleFor(request => request.TaskRequests).NotEmpty();
        }
    }
}
