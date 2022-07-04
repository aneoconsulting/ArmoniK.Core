using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Principal;
using System.Text;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Auth.Types;

using Microsoft.AspNetCore.Identity;

namespace ArmoniK.Core.Common.Auth
{
  internal interface IAuthenticationSource
  {
    public bool ValidateCN(string CN);

    public IIdentity GetIdentity(string CN,
                                 string fingerprint);
  }
}
