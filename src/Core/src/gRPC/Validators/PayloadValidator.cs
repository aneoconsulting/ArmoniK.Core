using ArmoniK.Core.gRPC.V1;

using FluentValidation;

namespace ArmoniK.Core.gRPC.Validators
{
  public class PayloadValidator : AbstractValidator<Payload>
  {
    public PayloadValidator()
    {
      RuleFor(o => o.Data).NotNull().NotEmpty();
    }
  }
}