using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.Auth
{
  internal class Authorizor : IAuthorizationHandler
  {
    public Task HandleAsync(AuthorizationHandlerContext context)
    {
      throw new NotImplementedException();
    }
  }
}
