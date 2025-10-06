// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2025. All rights reserved.
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

using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Tests.TestBase;

using Microsoft.Extensions.DependencyInjection;

using MongoDB.Bson.Serialization;

namespace ArmoniK.Core.Adapters.MongoDB.Tests;

public class AuthenticationTableTest : AuthenticationTableTestBase
{
  public override void TearDown()
  {
    tableProvider_?.Dispose();
    RunTests = false;
  }

  private MongoDatabaseProvider? tableProvider_;

  static AuthenticationTableTest()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(UserAuthenticationResult)))
    {
      BsonClassMap.RegisterClassMap<UserAuthenticationResult>(cm =>
                                                              {
                                                                cm.MapIdProperty(nameof(UserAuthenticationResult.Id))
                                                                  .SetSerializer(IdSerializer.Instance)
                                                                  .SetShouldSerializeMethod(_ => true)
                                                                  .SetIsRequired(true);
                                                                cm.MapProperty(nameof(UserAuthenticationResult.Username))
                                                                  .SetIsRequired(true);
                                                                cm.MapProperty(nameof(UserAuthenticationResult.Roles))
                                                                  .SetIgnoreIfDefault(true)
                                                                  .SetDefaultValue(Array.Empty<string>());
                                                                cm.MapProperty(nameof(UserAuthenticationResult.Permissions))
                                                                  .SetIgnoreIfDefault(true)
                                                                  .SetDefaultValue(Array.Empty<string>());
                                                                cm.MapCreator(model => new UserAuthenticationResult(model.Id,
                                                                                                                    model.Username,
                                                                                                                    model.Roles,
                                                                                                                    model.Permissions));
                                                              });
    }
  }

  public override void GetAuthSource()
  {
    tableProvider_ = new MongoDatabaseProvider();
    var provider = tableProvider_.GetServiceProvider();

    AuthenticationTable = provider.GetRequiredService<IAuthenticationTable>();
    RunTests            = true;
  }
}
