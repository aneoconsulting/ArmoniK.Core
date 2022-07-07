using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.Auth.Authorization
{
  internal class HasActionAccessHandler : AuthorizationHandler<HasActionAccessRequirement>
  {
    protected override Task HandleRequirementAsync(AuthorizationHandlerContext context,
                                                   HasActionAccessRequirement  requirement)
      => throw new NotImplementedException();
  }
}
