// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.PostgreSQL.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.PostgreSQL.Tests;

/// <summary>
///   Seeding of the authentication tables by <see cref="NpgsqlConnectionProvider.Init" /> when the authentication
///   configuration changes between two initializations of the same database
/// </summary>
[TestFixture]
public class AuthenticationInitTests
{
  [SetUp]
  public void SetUp()
  {
    tableProvider_ = new PostgresDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();
    options_        = provider.GetRequiredService<Options.PostgreSQL>();
    activitySource_ = provider.GetRequiredService<ActivitySource>();
  }

  [TearDown]
  public void TearDown()
    => tableProvider_?.Dispose();

  private PostgresDatabaseProvider? tableProvider_;
  private Options.PostgreSQL?       options_;
  private ActivitySource?           activitySource_;

  private static readonly Role Monitoring = new()
                                            {
                                              Name        = "Monitoring",
                                              Permissions = ["Sessions:ListSessions"],
                                            };

  private static readonly Role Submitter = new()
                                           {
                                             Name        = "Submitter",
                                             Permissions = ["Tasks:SubmitTasks"],
                                           };

  // Sorts before the existing roles, which shifts their position-based ids
  private static readonly Role Admin = new()
                                       {
                                         Name        = "Admin",
                                         Permissions = ["Sessions:CreateSession", "Tasks:CancelTasks"],
                                       };

  private static InitDatabase BuildInitDatabase(IEnumerable<Role>        roles,
                                                IEnumerable<User>        users,
                                                IEnumerable<Certificate> certificates)
    => new(new InitServices
           {
             Authentication = new Authentication
                              {
                                Roles = roles.Select(role => role.ToJson())
                                             .ToList(),
                                Users = users.Select(user => user.ToJson())
                                             .ToList(),
                                UserCertificates = certificates.Select(certificate => certificate.ToJson())
                                                               .ToList(),
                              },
           });

  /// <summary>
  ///   Runs the initialization of the shared test database with the given configuration, as an init job would do
  ///   on each deployment
  /// </summary>
  private async Task InitWith(InitDatabase initDatabase)
  {
    await using var provider = new NpgsqlConnectionProvider(options_!,
                                                            initDatabase,
                                                            NullLogger<NpgsqlConnectionProvider>.Instance);
    await provider.Init(CancellationToken.None)
                  .ConfigureAwait(false);
  }

  private static InitDatabase EmptyInitDatabase()
    => BuildInitDatabase([],
                         [],
                         []);

  /// <summary>
  ///   Reads the database through <see cref="AuthenticationTable" />, with a provider whose initialization does not
  ///   seed authentication data
  /// </summary>
  private async Task<T> Query<T>(Func<AuthenticationTable, Task<T>> query)
  {
    await using var provider = new NpgsqlConnectionProvider(options_!,
                                                            EmptyInitDatabase(),
                                                            NullLogger<NpgsqlConnectionProvider>.Instance);
    await provider.Init(CancellationToken.None)
                  .ConfigureAwait(false);
    return await query(new AuthenticationTable(provider,
                                               activitySource_!))
             .ConfigureAwait(false);
  }

  private Task<UserAuthenticationResult?> FromUser(string username)
    => Query(table => table.GetIdentityFromUserAsync(null,
                                                     username));

  private Task<UserAuthenticationResult?> FromCertificate(string cn)
    => Query(table => table.GetIdentityFromCertificateAsync(cn,
                                                            "fingerprint"));

  private static User UserWith(string        name,
                               params Role[] roles)
    => new()
       {
         Name = name,
         Roles = roles.Select(role => role.Name)
                      .ToList(),
       };

  private static Certificate CertificateOf(User   user,
                                           string cn)
    => new()
       {
         User = user.Name,
         Cn   = cn,
       };

  [Test]
  public async Task UserShouldGetTheRoleAddedBeforeExistingRoles()
  {
    await InitWith(BuildInitDatabase([Monitoring, Submitter],
                                     [],
                                     []))
      .ConfigureAwait(false);

    await InitWith(BuildInitDatabase([Admin, Monitoring, Submitter],
                                     [
                                       UserWith("user",
                                                Admin),
                                     ],
                                     []))
      .ConfigureAwait(false);

    var identity = await FromUser("user")
                     .ConfigureAwait(false);

    Assert.That(identity?.Roles,
                Is.EquivalentTo(new[]
                                {
                                  Admin.Name,
                                }));
    Assert.That(identity?.Permissions,
                Is.EquivalentTo(Admin.Permissions));
  }

  [Test]
  public async Task CertificateShouldMapToTheUserAddedBeforeExistingUsers()
  {
    var userB = UserWith("userB",
                         Submitter);
    var certificateB = CertificateOf(userB,
                                     "cnB");

    await InitWith(BuildInitDatabase([Submitter],
                                     [userB],
                                     [certificateB]))
      .ConfigureAwait(false);

    // userA sorts before userB, and cnA before cnB: both shift the position-based ids
    var userA = UserWith("userA",
                         Submitter);
    var certificateA = CertificateOf(userA,
                                     "cnA");

    await InitWith(BuildInitDatabase([Submitter],
                                     [userA, userB],
                                     [certificateA, certificateB]))
      .ConfigureAwait(false);

    Assert.That((await FromCertificate(certificateA.Cn)
                   .ConfigureAwait(false))?.Username,
                Is.EqualTo(userA.Name));
    Assert.That((await FromCertificate(certificateB.Cn)
                   .ConfigureAwait(false))?.Username,
                Is.EqualTo(userB.Name));
  }

  [Test]
  public async Task RemovedCertificateShouldNoLongerAuthenticate()
  {
    var userA = UserWith("userA",
                         Submitter);
    var userB = UserWith("userB",
                         Admin);
    var certificateA = CertificateOf(userA,
                                     "cnA");
    var certificateB = CertificateOf(userB,
                                     "cnB");

    await InitWith(BuildInitDatabase([Admin, Submitter],
                                     [userA, userB],
                                     [certificateA, certificateB]))
      .ConfigureAwait(false);

    await InitWith(BuildInitDatabase([Submitter],
                                     [userA],
                                     [certificateA]))
      .ConfigureAwait(false);

    Assert.That(await FromCertificate(certificateB.Cn)
                  .ConfigureAwait(false),
                Is.Null);
    Assert.That(await FromUser(userB.Name)
                  .ConfigureAwait(false),
                Is.Null);

    var identityA = await FromCertificate(certificateA.Cn)
                      .ConfigureAwait(false);
    Assert.That(identityA?.Username,
                Is.EqualTo(userA.Name));
    Assert.That(identityA?.Permissions,
                Is.EquivalentTo(Submitter.Permissions));
  }

  [Test]
  public async Task UserShouldKeepItsRoleWhenAnotherRoleIsRemoved()
  {
    await InitWith(BuildInitDatabase([Admin, Monitoring, Submitter],
                                     [
                                       UserWith("user",
                                                Monitoring),
                                     ],
                                     []))
      .ConfigureAwait(false);

    await InitWith(BuildInitDatabase([Monitoring, Submitter],
                                     [
                                       UserWith("user",
                                                Monitoring),
                                     ],
                                     []))
      .ConfigureAwait(false);

    var identity = await FromUser("user")
                     .ConfigureAwait(false);

    Assert.That(identity?.Roles,
                Is.EquivalentTo(new[]
                                {
                                  Monitoring.Name,
                                }));
    Assert.That(identity?.Permissions,
                Is.EquivalentTo(Monitoring.Permissions));
  }

  [Test]
  public async Task InitWithoutAuthenticationConfigurationShouldKeepExistingData()
  {
    var user = UserWith("user",
                        Admin);
    var certificate = CertificateOf(user,
                                    "cn");

    await InitWith(BuildInitDatabase([Admin],
                                     [user],
                                     [certificate]))
      .ConfigureAwait(false);

    // Another init process, e.g. the compute plane one, seeds the database without the authentication configuration
    await InitWith(EmptyInitDatabase())
      .ConfigureAwait(false);

    var identity = await FromCertificate(certificate.Cn)
                     .ConfigureAwait(false);

    Assert.That(identity?.Username,
                Is.EqualTo(user.Name));
    Assert.That(identity?.Permissions,
                Is.EquivalentTo(Admin.Permissions));
  }
}
