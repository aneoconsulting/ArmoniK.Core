// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
// 
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
// 
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections.Generic;
using System.Diagnostics;

using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Tests.Auth;

using Microsoft.Extensions.DependencyInjection;

using Mongo2Go;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

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
