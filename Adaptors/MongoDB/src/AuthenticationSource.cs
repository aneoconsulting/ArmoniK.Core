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
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Auth.Authorization;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

public class AuthenticationSource : IAuthenticationSource
{
  private readonly ActivitySource                                          activitySource_;
  private readonly MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider_;
  private readonly MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider_;
  private          bool                                                    isInitialized_;

  private readonly PipelineDefinition<UserData, UserIdentityResult>      userToIdentityPipeline_;
  private readonly PipelineDefinition<AuthData, UserIdentityResult>      authToIdentityPipeline_;

  public AuthenticationSource(SessionProvider                                         sessionProvider,
                              MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider,
                              MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider,
                              MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider,
                              ILogger<AuthenticationSource>                           logger,
                              ActivitySource                                          activitySource)
  {
    sessionProvider_          = sessionProvider;
    userCollectionProvider_   = userCollectionProvider;
    roleCollectionProvider_   = roleCollectionProvider;
    authCollectionProvider_   = authCollectionProvider;

    Logger                  = logger;
    activitySource_         = activitySource;
    userToIdentityPipeline_ = CreateUserToIdentityPipeline();
    authToIdentityPipeline_ = CreateAuthToIdentityPipeline(userToIdentityPipeline_);
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

  [ItemCanBeNull]
  private static async Task<UserIdentity> GetIdentityFromPipeline<TCollectionDataType>(IClientSessionHandle sessionHandle, IMongoCollection<TCollectionDataType> collection, PipelineDefinition<TCollectionDataType, UserIdentityResult> pipeline, Expression<Func<TCollectionDataType, bool>> matchingFunctions, CancellationToken cancellationToken)
  {
    var res = await collection.AggregateAsync(sessionHandle,
                                                  new PrependedStagePipelineDefinition<TCollectionDataType, TCollectionDataType, UserIdentityResult>(PipelineStageDefinitionBuilder
                                                                                                                                                       .Match(matchingFunctions),
                                                                                                                                                     pipeline),
                                                  new AggregateOptions(),
                                                  cancellationToken)
                                  .ConfigureAwait(false);
    await foreach (var ident in res.Current.ToAsyncEnumerable()
                                   .WithCancellation(cancellationToken)
                                   .ConfigureAwait(false))
    {
      return new UserIdentity(ident.UserId,
                              ident.Username,
                              ident.Roles,
                              ident.Permissions.Select(str => new Permissions.Permission(str)));
    }
    return null;
  }

  [ItemCanBeNull]
  public async Task<UserIdentity> GetIdentityAsync(string            cn,
                                                   string            fingerprint,
                                                   CancellationToken cancellationToken)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetIdentityAsync)}");
    var       authCollection = authCollectionProvider_.Get();
    var       sessionHandle  = sessionProvider_.Get();
    return await GetIdentityFromPipeline(sessionHandle,
                                         authCollection,
                                         authToIdentityPipeline_,
                                         auth => auth.CN == cn && auth.Fingerprint == fingerprint,
                                         cancellationToken)
             .ConfigureAwait(false);
  }

  [ItemCanBeNull]
  public async Task<UserIdentity> GetIdentityFromIdAsync(string            id,
                                                         CancellationToken cancellationToken)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromIdAsync)}");
    var       userCollection = userCollectionProvider_.Get();
    var       sessionHandle  = sessionProvider_.Get();
    return await GetIdentityFromPipeline(sessionHandle,
                                         userCollection,
                                         userToIdentityPipeline_,
                                         user => user.UserId == id,
                                         cancellationToken)
             .ConfigureAwait(false);

  }

  [ItemCanBeNull]
  public async Task<UserIdentity> GetIdentityFromNameAsync(string            username,
                                                           CancellationToken cancellationToken)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromIdAsync)}");
    var       userCollection = userCollectionProvider_.Get();
    var       sessionHandle  = sessionProvider_.Get();
    return await GetIdentityFromPipeline(sessionHandle,
                                         userCollection,
                                         userToIdentityPipeline_,
                                         user => user.Username == username,
                                         cancellationToken)
             .ConfigureAwait(false);
  }


  private PipelineDefinition<UserData, UserIdentityResult> CreateUserToIdentityPipeline()
  => new BsonDocument[]
     {
       new("$lookup",
             new BsonDocument
             {
               {
                 "from", roleCollectionProvider_.Get()
                                                .CollectionNamespace.CollectionName
               },
               {
                 "localField", nameof(UserData.Roles)
               },
               {
                 "foreignField", "_id"
               },
               {
                 "as", nameof(UserData.Roles)
               },
             }),
         new("$project",
             new BsonDocument
             {
               {
                 nameof(UserIdentityResult.Username), $"${nameof(UserData.Username)}"
               },
               {
                 nameof(UserData), "$$REMOVE"
               },
               {
                 nameof(UserIdentityResult.Roles), $"${nameof(UserData.Roles)}.{nameof(RoleData.RoleName)}"
               },
               {
                 nameof(UserIdentityResult.Permissions), new BsonDocument("$reduce",
                                                                          new BsonDocument
                                                                          {
                                                                            {
                                                                              "input", $"${nameof(UserData.Roles)}.{nameof(RoleData.Permissions)}"
                                                                            },
                                                                            {
                                                                              "initialValue", new BsonArray()
                                                                            },
                                                                            {
                                                                              "in", new BsonDocument("$setUnion",
                                                                                                     new BsonArray
                                                                                                     {
                                                                                                       "$$this",
                                                                                                       "$$value",
                                                                                                     })
                                                                            },
                                                                          })
               },
             })
       };

  private PipelineDefinition<AuthData, UserIdentityResult> CreateAuthToIdentityPipeline(PipelineDefinition<UserData, UserIdentityResult> userToIdentityPipeline)
  {
    var head = new BsonDocument[]
               {
                 new("$lookup",
                     new BsonDocument
                     {
                       {
                         "from", userCollectionProvider_.Get()
                                                        .CollectionNamespace.CollectionName
                       },
                       {
                         "localField", nameof(AuthData.UserId)
                       },
                       {
                         "foreignField", "_id"
                       },
                       {
                         "as", nameof(UserData)
                       },
                     }),
                 new("$replaceRoot",
                     new BsonDocument(
                                      "$arrayElemAt", new BsonArray
                                                      {
                                                        $"${nameof(UserData)}",
                                                        0,
                                                      })),
               };

    return head.Concat(userToIdentityPipeline.Stages.Select(s => s.ToBsonDocument())).ToArray();
  }
}
