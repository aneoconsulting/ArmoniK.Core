using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Common.Auth.Authentication;

using Microsoft.AspNetCore.Identity;

namespace ArmoniK.Core.Common.Storage
{
  public interface IPermissionTable : IInitializable
  {
    public Task<IList<User>>  ListUsersAsync(CancellationToken cancellationToken);
    public Task<IList<User>>  ListUsersAsync(string            cn,   CancellationToken cancellationToken);

    public Task<User?> GetUserAsync(string cn, string fingerprint, CancellationToken cancellationToken);

    public Task<IList<string>> GetRolesAsync(string            cn,
                                        string            fingerprint,
                                        CancellationToken cancellationToken);

    public Task<IList<Claim>> GetClaimsAsync(User              user,
                                        CancellationToken cancellationToken);





  }
}
