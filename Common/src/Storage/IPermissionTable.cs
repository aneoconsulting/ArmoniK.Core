using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ArmoniK.Core.Common.Auth.Types;
using Microsoft.AspNetCore.Identity;

namespace ArmoniK.Core.Common.Storage
{
  public interface IPermissionTable : IInitializable
  {
    public Task<IList<User>>  ListUsers(CancellationToken cancellationToken);
    public Task<IList<User>>  ListUsers(string            cn,   CancellationToken cancellationToken);
    public Task<IList<Claim>> GetClaims(User              user, CancellationToken cancellationToken);
    public Task<IList<string>> GetRoles(User              user,
                                      CancellationToken cancellationToken);
    public Task<bool> HasRole(User user, string role, CancellationToken cancellationToken);

    public Task<bool> HasClaim(User user, string claim, CancellationToken cancellationToken);



  }
}
