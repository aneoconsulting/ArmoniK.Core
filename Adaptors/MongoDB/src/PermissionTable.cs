using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;

using Microsoft.AspNetCore.Identity;

namespace ArmoniK.Core.Adapters.MongoDB
{
  public class PermissionTable : IPermissionTable
  {
    public ValueTask<bool> Check(HealthCheckTag tag)
      => throw new NotImplementedException();

    public Task Init(CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<IList<User>> ListUsersAsync(CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<IList<User>> ListUsersAsync(string            cn,
                                            CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<User> GetUserAsync(string            cn,
                                   string            fingerprint,
                                   CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<IList<string>> GetRolesAsync(string            cn,
                                             string            fingerprint,
                                             CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public Task<IList<Claim>> GetClaimsAsync(User              user,
                                             CancellationToken cancellationToken)
      => throw new NotImplementedException();
  }
}
