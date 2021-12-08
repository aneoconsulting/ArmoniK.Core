using ArmoniK.Core.gRPC.V1;

using Calzolari.Grpc.AspNetCore.Validation;

using FluentValidation;
using FluentValidation.Results;

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

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