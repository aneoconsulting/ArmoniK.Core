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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Injection.Options;
using ArmoniK.Core.Common.Injection.Options.Database;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Driver;

using NUnit.Framework;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

/// <summary>
///   Seeding of the authentication collections when the authentication configuration changes between two
///   initializations of the same database
/// </summary>
[TestFixture]
internal class AuthenticationInitTests
{
  [SetUp]
  public void SetUp()
  {
    dbProvider_ = new MongoDatabaseProvider();
    var provider = dbProvider_.GetServiceProvider();
    database_ = provider.GetRequiredService<IMongoDatabase>();
    client_   = provider.GetRequiredService<IMongoClient>();
    options_  = provider.GetRequiredService<Options.MongoDB>();
  }

  [TearDown]
  public void TearDown()
    => dbProvider_?.Dispose();

  private MongoDatabaseProvider? dbProvider_;
  private IMongoDatabase?        database_;
  private IMongoClient?          client_;
  private Options.MongoDB?       options_;

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

  // Sorts before the other roles, which shifts their position-based ids
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

  private async Task<IMongoCollection<T>> GetCollection<T, TMapping>()
    where TMapping : IMongoDataModelMapping<T>, new()
  {
    var mapping    = new TMapping();
    var collection = database_!.GetCollection<T>(mapping.CollectionName);
    await collection.Indexes.CreateManyAsync(mapping.InitializeIndexes(options_!))
                    .ConfigureAwait(false);
    return collection;
  }

  private async Task Seed<T, TMapping>(InitDatabase initDatabase)
    where TMapping : IMongoDataModelMapping<T>, new()
  {
    var collection = await GetCollection<T, TMapping>()
                       .ConfigureAwait(false);
    using var session = await client_!.StartSessionAsync()
                                      .ConfigureAwait(false);
    await new TMapping().InitializeCollectionAsync(session,
                                                   collection,
                                                   initDatabase)
                        .ConfigureAwait(false);
  }

  /// <summary>
  ///   Seeds the three authentication collections, as an init process would do on each deployment
  /// </summary>
  private async Task InitWith(InitDatabase initDatabase)
  {
    await Seed<RoleData, RoleDataModelMapping>(initDatabase)
      .ConfigureAwait(false);
    await Seed<UserData, UserDataModelMapping>(initDatabase)
      .ConfigureAwait(false);
    await Seed<AuthData, AuthDataModelMapping>(initDatabase)
      .ConfigureAwait(false);
  }

  private async Task<List<T>> ReadAll<T, TMapping>()
    where TMapping : IMongoDataModelMapping<T>, new()
  {
    var collection = await GetCollection<T, TMapping>()
                       .ConfigureAwait(false);
    return await collection.Find(Builders<T>.Filter.Empty)
                           .ToListAsync()
                           .ConfigureAwait(false);
  }

  /// <summary>
  ///   The collections must hold exactly the entries of the configuration, so that the ids referenced by users and
  ///   certificates resolve to the intended roles and users
  /// </summary>
  private async Task AssertCollectionsMatch(InitDatabase expected)
  {
    var roles = await ReadAll<RoleData, RoleDataModelMapping>()
                  .ConfigureAwait(false);
    var users = await ReadAll<UserData, UserDataModelMapping>()
                  .ConfigureAwait(false);
    var auths = await ReadAll<AuthData, AuthDataModelMapping>()
                  .ConfigureAwait(false);

    Assert.That(roles.Select(role => (role.RoleId, role.RoleName, string.Join(",",
                                                                              role.Permissions))),
                Is.EquivalentTo(expected.Roles.Select(role => (role.RoleId, role.RoleName, string.Join(",",
                                                                                                       role.Permissions)))));
    Assert.That(users.Select(user => (user.UserId, user.Username, string.Join(",",
                                                                              user.Roles))),
                Is.EquivalentTo(expected.Users.Select(user => (user.UserId, user.Username, string.Join(",",
                                                                                                       user.Roles)))));
    Assert.That(auths,
                Is.EquivalentTo(expected.Auths));
  }

  [Test]
  public async Task CollectionsShouldFollowAddedEntries()
  {
    var userB = UserWith("userB",
                         Submitter);

    await InitWith(BuildInitDatabase([Monitoring, Submitter],
                                     [userB],
                                     [
                                       CertificateOf(userB,
                                                     "cnB"),
                                     ]))
      .ConfigureAwait(false);

    var userA = UserWith("userA",
                         Admin);
    var expected = BuildInitDatabase([Admin, Monitoring, Submitter],
                                     [userA, userB],
                                     [
                                       CertificateOf(userA,
                                                     "cnA"),
                                       CertificateOf(userB,
                                                     "cnB"),
                                     ]);

    await InitWith(expected)
      .ConfigureAwait(false);

    await AssertCollectionsMatch(expected)
      .ConfigureAwait(false);
  }

  [Test]
  public async Task CollectionsShouldFollowRemovedEntries()
  {
    var userA = UserWith("userA",
                         Admin);
    var userB = UserWith("userB",
                         Submitter);

    await InitWith(BuildInitDatabase([Admin, Monitoring, Submitter],
                                     [userA, userB],
                                     [
                                       CertificateOf(userA,
                                                     "cnA"),
                                       CertificateOf(userB,
                                                     "cnB"),
                                     ]))
      .ConfigureAwait(false);

    // Removing the first entry of each list moves the remaining ones to lower ids, which used to break the unique
    // indexes on the names and on (fingerprint, common name)
    var expected = BuildInitDatabase([Monitoring, Submitter],
                                     [userB],
                                     [
                                       CertificateOf(userB,
                                                     "cnB"),
                                     ]);

    await InitWith(expected)
      .ConfigureAwait(false);

    await AssertCollectionsMatch(expected)
      .ConfigureAwait(false);
  }

  [Test]
  public async Task InitWithoutAuthenticationConfigurationShouldKeepExistingData()
  {
    var user = UserWith("user",
                        Admin);
    var expected = BuildInitDatabase([Admin],
                                     [user],
                                     [
                                       CertificateOf(user,
                                                     "cn"),
                                     ]);

    await InitWith(expected)
      .ConfigureAwait(false);

    // Another init process, e.g. the compute plane one, seeds the database without the authentication configuration
    await InitWith(BuildInitDatabase([],
                                     [],
                                     []))
      .ConfigureAwait(false);

    await AssertCollectionsMatch(expected)
      .ConfigureAwait(false);
  }
}
