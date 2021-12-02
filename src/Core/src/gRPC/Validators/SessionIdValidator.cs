
using ArmoniK.Core.gRPC.V1;
using FluentValidation;

namespace ArmoniK.Core.gRPC
{
    public class SessionIdValidator : AbstractValidator<SessionId>
    {
        public SessionIdValidator()
        {
            RuleFor(o => o.Session).NotNull();
        }
    }
}
