using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.AspNetCore.Authorization;

namespace ArmoniK.Core.Common.Auth.Authorization
{
  internal class HasActionAccessRequirement : IAuthorizationRequirement
  {
    public string Action { get; }

    public HasActionAccessRequirement(string action)
    {
      Action = action;
    }
  }
}
