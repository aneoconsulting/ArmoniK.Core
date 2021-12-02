using ArmoniK.Core.gRPC.V1;
using FluentValidation;

namespace ArmoniK.Core.gRPC.Validators
{
    public class SessionOptionsValidator : AbstractValidator<SessionOptions>
    {
        public SessionOptionsValidator()
        {
            RuleFor(o => o.DefaultTaskOption).NotNull().SetValidator(new TaskOptionsValidator());
        }
    }
}
