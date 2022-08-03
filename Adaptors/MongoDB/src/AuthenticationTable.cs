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

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Auth.Authentication;

using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using MongoDB.Bson.Serialization;

namespace ArmoniK.Core.Adapters.MongoDB;

public class AuthenticationTable : IAuthenticationTable
{
  private readonly ActivitySource                                          activitySource_;
  private readonly MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider_;
  private readonly MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider_;
  private          bool                                                    isInitialized_;

  private PipelineDefinition<UserData, UserAuthenticationResult>? userToIdentityPipeline_;
  private PipelineDefinition<AuthData, UserAuthenticationResult>? authToIdentityPipeline_;

  static AuthenticationTable()
  {
    if (!BsonClassMap.IsClassMapRegistered(typeof(UserAuthenticationResult)))
    {
      BsonClassMap.RegisterClassMap<UserAuthenticationResult>(cm =>
                                                              {
                                                                cm.MapIdProperty(nameof(UserAuthenticationResult.Id))
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

  public AuthenticationTable(SessionProvider                                         sessionProvider,
                             MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider,
                             MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider,
                             MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider,
                             ILogger<AuthenticationTable>                            logger,
                             ActivitySource                                          activitySource)
  {
    sessionProvider_        = sessionProvider;
    userCollectionProvider_ = userCollectionProvider;
    roleCollectionProvider_ = roleCollectionProvider;
    authCollectionProvider_ = authCollectionProvider;

    Logger          = logger;
    activitySource_ = activitySource;
  }

  public ILogger Logger { get; }

  /// <inheritdoc />
  public ValueTask<bool> Check(HealthCheckTag tag)
    => ValueTask.FromResult(isInitialized_);

  /// <inheritdoc />
  public Task Init(CancellationToken cancellationToken)
  {
    if (!isInitialized_)
    {
      sessionProvider_.Get();
      userCollectionProvider_.Get();
      roleCollectionProvider_.Get();
      authCollectionProvider_.Get();
      isInitialized_ = true;
    }

    return Task.CompletedTask;
  }

  private static async Task<UserAuthenticationResult?> GetIdentityFromPipelineAsync<TCollectionDataType>(IClientSessionHandle                  sessionHandle,
                                                                                                         IMongoCollection<TCollectionDataType> collection,
                                                                                                         PipelineDefinition<TCollectionDataType,
                                                                                                           UserAuthenticationResult> pipeline,
                                                                                                         Expression<Func<TCollectionDataType, bool>> matchingFunctions,
                                                                                                         CancellationToken cancellationToken = default)
  {
    var pipe =
      new PrependedStagePipelineDefinition<TCollectionDataType, TCollectionDataType, UserAuthenticationResult>(PipelineStageDefinitionBuilder.Match(matchingFunctions),
                                                                                                               pipeline);

    return await collection.AggregateAsync(sessionHandle,
                                           pipe,
                                           new AggregateOptions(),
                                           cancellationToken)
                           .ContinueWith(task => task.Result.FirstOrDefault(),
                                         cancellationToken)
                           .ConfigureAwait(false);
  }

  public async Task<UserAuthenticationResult?> GetIdentityFromCertificateAsync(string            cn,
                                                                               string            fingerprint,
                                                                               CancellationToken cancellationToken = default)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromCertificateAsync)}");
    var       authCollection = authCollectionProvider_.Get();
    var       sessionHandle  = sessionProvider_.Get();
    return await GetIdentityFromPipelineAsync(sessionHandle,
                                              authCollection,
                                              GetAuthToIdentityPipeline(),
                                              auth => auth.CN == cn && (auth.Fingerprint == fingerprint || auth.Fingerprint == null),
                                              cancellationToken)
             .ConfigureAwait(false);
  }

  public async Task<UserAuthenticationResult?> GetIdentityFromUserAsync(string?           id,
                                                                        string?           username,
                                                                        CancellationToken cancellationToken = default)
  {
    using var                        activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromUserAsync)}");
    var                              userCollection = userCollectionProvider_.Get();
    var                              sessionHandle  = sessionProvider_.Get();
    Expression<Func<UserData, bool>> expression;
    if (id != null)
    {
      expression = data => data.UserId == id;
    }
    else if (username != null)
    {
      expression = data => data.Username == username;
    }
    else
    {
      return null;
    }

    return await GetIdentityFromPipelineAsync(sessionHandle,
                                              userCollection,
                                              GetUserToIdentityPipeline(),
                                              expression,
                                              cancellationToken)
             .ConfigureAwait(false);
  }


  private PipelineDefinition<UserData, UserAuthenticationResult> GetUserToIdentityPipeline()
  {
    if (userToIdentityPipeline_ != null)
    {
      return userToIdentityPipeline_;
    }

    var lookup = PipelineStageDefinitionBuilder.Lookup<UserData, RoleData, UserDataAfterLookup>(roleCollectionProvider_.Get(),
                                                                                                u => u.Roles,
                                                                                                r => r.RoleId,
                                                                                                ual => ual.Roles);
    var projectionStage = PipelineStageDefinitionBuilder.Project<UserDataAfterLookup, UserAuthenticationResult>(ual => new UserAuthenticationResult(ual.UserId,
                                                                                                                                                    ual.Username,
                                                                                                                                                    ual.Roles
                                                                                                                                                       .Select(r => r
                                                                                                                                                                 .RoleName),
                                                                                                                                                    ual.Roles
                                                                                                                                                       .Aggregate<
                                                                                                                                                         RoleData,
                                                                                                                                                         IEnumerable<
                                                                                                                                                           string>>(new
                                                                                                                                                                      HashSet
                                                                                                                                                                      <string>(),
                                                                                                                                                                    (set,
                                                                                                                                                                     data)
                                                                                                                                                                      => set
                                                                                                                                                                        .Union(data
                                                                                                                                                                                 .Permissions))));
    var pipeline = new IPipelineStageDefinition[]
                   {
                     lookup,
                     projectionStage,
                   };
    userToIdentityPipeline_ = new PipelineStagePipelineDefinition<UserData, UserAuthenticationResult>(pipeline);
    return userToIdentityPipeline_;
  }

  private PipelineDefinition<AuthData, UserAuthenticationResult> GetAuthToIdentityPipeline()
  {
    if (authToIdentityPipeline_ != null)
    {
      return authToIdentityPipeline_;
    }

    var userToIdentityPipeline = GetUserToIdentityPipeline();
    var sortByRelevance        = PipelineStageDefinitionBuilder.Sort(new SortDefinitionBuilder<AuthData>().Descending(authData => authData.Fingerprint));
    var limit                  = PipelineStageDefinitionBuilder.Limit<AuthData>(1);
    var lookup = PipelineStageDefinitionBuilder.Lookup<AuthData, UserData, AuthDataAfterLookup>(userCollectionProvider_.Get(),
                                                                                                auth => auth.UserId,
                                                                                                user => user.UserId,
                                                                                                authAfterLookup => authAfterLookup.UserData);
    var checkIfValid = PipelineStageDefinitionBuilder.Match<AuthDataAfterLookup>(doc => doc.UserData.Any());
    var replaceRoot  = PipelineStageDefinitionBuilder.ReplaceRoot<AuthDataAfterLookup, UserData>(doc => doc.UserData.First());
    var pipeline = new IPipelineStageDefinition[]
                   {
                     sortByRelevance,
                     limit,
                     lookup,
                     checkIfValid,
                     replaceRoot,
                   }.Concat(userToIdentityPipeline.Stages);
    authToIdentityPipeline_ = new PipelineStagePipelineDefinition<AuthData, UserAuthenticationResult>(pipeline);
    return authToIdentityPipeline_;
  }

  public void AddRoles(IEnumerable<RoleData> roles)
  {
    var roleCollection = roleCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    roleCollection.InsertMany(sessionHandle,
                              roles);
  }

  public void AddUsers(IEnumerable<UserData> users)
  {
    var userCollection = userCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    userCollection.InsertMany(sessionHandle,
                              users);
  }

  public void AddCertificates(IEnumerable<AuthData> certificates)
  {
    var authCollection = authCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    authCollection.InsertMany(sessionHandle,
                              certificates);
  }
}
