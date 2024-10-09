// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2024. All rights reserved.
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

using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;
using ArmoniK.Utils;

namespace ArmoniK.Core.Common.Injection.Options.Database;

/// <summary>
///   Convert <see cref="InitServices" /> into objects we can insert into the database
///   Also used to init database collections
/// </summary>
public class InitDatabase
{
  /// <summary>
  ///   Collection of data to insert in the database for Authentication during ArmoniK initialization
  /// </summary>
  public readonly ICollection<AuthData> Auths;

  /// <summary>
  ///   Whether to init the database
  /// </summary>
  public readonly bool Init;

  /// <summary>
  ///   Collection of data to insert in the database for Partitions during ArmoniK initialization
  /// </summary>
  public readonly ICollection<PartitionData> Partitions;

  /// <summary>
  ///   Collection of data to insert in the database for Roles during ArmoniK initialization
  /// </summary>
  public readonly ICollection<RoleData> Roles;

  /// <summary>
  ///   Collection of data to insert in the database for Users during ArmoniK initialization
  /// </summary>
  public readonly ICollection<UserData> Users;


  /// <summary>
  ///   Instantiate <see cref="InitDatabase" /> from the configurations received from the Dependency Injection
  /// </summary>
  /// <param name="initServices">Data structure containing the raw data</param>
  public InitDatabase(InitServices initServices)
  {
    Init = initServices.InitDatabase;

    Roles = initServices.Authentication.Roles.Select(Role.FromJson)
                        .OrderBy(role => role.Name)
                        .Select((role,
                                 i) => new RoleData(i.ToString(),
                                                    role.Name,
                                                    role.Permissions.ToArray()))
                        .AsICollection();

    var roleDic = Roles.ToDictionary(data => data.RoleName,
                                     data => data.RoleId);

    Users = initServices.Authentication.Users.Select(User.FromJson)
                        .OrderBy(user => user.Name)
                        .Select((user,
                                 i) => new UserData(i.ToString(),
                                                    user.Name,
                                                    user.Roles.Select(roleName => roleDic[roleName])
                                                        .ToArray()))
                        .AsICollection();

    var userDic = Users.ToDictionary(data => data.Username,
                                     data => data.UserId);

    Auths = initServices.Authentication.UserCertificates.Select(Certificate.FromJson)
                        .OrderBy(certificate => (certificate.Fingerprint, certificate.CN))
                        .Select((certificate,
                                 i) => new AuthData(i.ToString(),
                                                    userDic[certificate.User],
                                                    certificate.CN,
                                                    certificate.Fingerprint))
                        .AsICollection();

    Partitions = initServices.Partitioning.Partitions.Select(Partition.FromJson)
                             .Select(partition => new PartitionData(partition.PartitionId,
                                                                    partition.ParentPartitionIds,
                                                                    partition.PodReserved,
                                                                    partition.PodMax,
                                                                    partition.PreemptionPercentage,
                                                                    partition.Priority,
                                                                    new PodConfiguration(partition.PodConfiguration)))
                             .AsICollection();
  }
}
