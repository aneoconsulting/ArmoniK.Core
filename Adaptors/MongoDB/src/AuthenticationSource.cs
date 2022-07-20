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
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace ArmoniK.Core.Adapters.MongoDB;

public class AuthenticationSource : IAuthenticationSource
{
  private readonly ActivitySource                                          activitySource_;
  private readonly MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider_;
  private readonly MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider_;
  private static   UserIdentityModelMapping                                mapping_;
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
    sessionProvider_        = sessionProvider;
    userCollectionProvider_ = userCollectionProvider;
    roleCollectionProvider_ = roleCollectionProvider;
    authCollectionProvider_ = authCollectionProvider;

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
    if(res.Current == null) return null;
    await foreach (var ident in res.Current.ToAsyncEnumerable()
                                   .WithCancellation(cancellationToken)
                                   .ConfigureAwait(false))
    {
      return new UserIdentity(ident.Id,
                              ident.Username,
                              ident.Roles,
                              ident.Permissions.Select(str => new Permissions.Permission(str)));
    }
    return null;
  }

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

  public async Task<UserIdentity> GetIdentityFromIdAsync(string            id,
                                                         CancellationToken cancellationToken)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromIdAsync)}");
    var       userCollection = userCollectionProvider_.Get();
    var       sessionHandle  = sessionProvider_.Get();
    /*var query2 = userCollectionProvider_.Get()
                                        .AsQueryable()
                                        .Where(u => u.UserId == id)
                                        .SelectMany(u=>u.Roles, (data,
                                                                                            s) => new {Id = data.UserId, data.Username, Role = s})
                                        .GroupJoin(roleCollectionProvider_.Get()
                                                                          .AsQueryable(),
                                                   ru => ru.Role,
                                                   r => r.RoleId,
                                                   (ru,
                                                    roles) => new UserIdentityResult(ru.Id,
                                                                                     ru.Username,
                                                                                     roles.Select(r => r.RoleName)
                                                                                          .ToArray(),
                                                                                     roles.SelectMany(r => r.Permissions)
                                                                                          .Distinct()
                                                                                          .ToArray()));*/

    IMongoQueryable<RoleData> inner = roleCollectionProvider_.Get()
                                                             .AsQueryable();
    var query3 = userCollectionProvider_.Get()
                                        .AsQueryable()
                                        .Where(u => u.UserId == id)
                                        .Select(u => new UserIdentityResult(u.UserId,
                                                                            u.Username,
                                                                            u.Roles.GroupJoin(inner,
                                                                                              s => s,
                                                                                              r => r.RoleId,
                                                                                              (s,
                                                                                               datas) => datas.First())
                                                                             .Select(r => r.RoleName)
                                                                             .ToArray(),
                                                                            u.Roles.GroupJoin(inner,
                                                                                              s => s,
                                                                                              r => r.RoleId,
                                                                                              (s,
                                                                                               datas) => datas.First())
                                                                             .SelectMany(r => r.Permissions)
                                                                             .Distinct()
                                                                             .ToArray()));
    //Console.WriteLine(((IMongoQueryable<UserData>)query2).ToJson());
    try
    {
      var result = await query3.ToAsyncEnumerable().FirstAsync(cancellationToken).ConfigureAwait(false);
      return null; /*new UserIdentity(result.Id,
                                   result.Username,
                                   result.Roles,
                                   result.Permissions.Select(p => new Permissions.Permission(p)));*/
    }
    catch (InvalidOperationException e)
    {
      Console.WriteLine($"invalid {e}");
      return null;
    }
    catch (ArgumentNullException e)
    {
      Console.WriteLine($"null {e}");
      return null;
    }

    /*return await GetIdentityFromPipeline(sessionHandle,
                                         userCollection,
                                         userToIdentityPipeline_,
                                         user => user.UserId == id,
                                         cancellationToken)
             .ConfigureAwait(false);
    */
  }

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
  {
    var lookup = PipelineStageDefinitionBuilder.Lookup<UserData, RoleData, UserDataAfterLookup>(roleCollectionProvider_.Get(),
                                                                                                u => u.Roles,
                                                                                                r => r.RoleId,
                                                                                                ual => ual.Roles);
    var projectionStage = PipelineStageDefinitionBuilder.Project<UserDataAfterLookup, UserIdentityResult>(ual => new(ual.UserId,
                                                                                                                     ual.Username,
                                                                                                                     ual.Roles.Select(r => r.RoleName),
                                                                                                                     ual.Roles
                                                                                                                        .Aggregate<RoleData,
                                                                                                                          IEnumerable<string>>(new HashSet<string>(),
                                                                                                                                               (set,
                                                                                                                                                data)
                                                                                                                                                 => set
                                                                                                                                                   .Union(data
                                                                                                                                                            .Permissions))));

    
    /*var query =
      from u in userCollectionProvider_.Get()
                                       .AsQueryable()
      join r in roleCollectionProvider_.Get() on u.Roles equals r.RoleId into UserData 
      select new UserIdentityResult(u.UserId,
                                    u.Username, UserData.Roles.Select(r=>r.RoleName),
                                    UserData.Aggregate<RoleData, ISet<string>, IList<string>>(new HashSet<string>(),
                                                                                              (cur,
                                                                                               r) =>
                                                                                              {
                                                                                                cur.UnionWith(r.Permissions);
                                                                                                return cur;
                                                                                              }, s => s.ToList()));
                  */
    var pipeline = new IPipelineStageDefinition[]
                   {
                     lookup,
                     projectionStage,
                   };
    return new PipelineStagePipelineDefinition<UserData, UserIdentityResult>(pipeline);
    //return null;
  }
  /*=> new BsonDocument[]
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
       };*/

  private PipelineDefinition<AuthData, UserIdentityResult> CreateAuthToIdentityPipeline(PipelineDefinition<UserData, UserIdentityResult> userToIdentityPipeline)
  {
    var lookup = PipelineStageDefinitionBuilder.Lookup<AuthData, UserData, AuthDataAfterLookup>(userCollectionProvider_.Get(),
                                                                                                a => a.UserId,
                                                                                                u => u.UserId,
                                                                                                aal => aal.UserData);
    var replaceroot = PipelineStageDefinitionBuilder.ReplaceRoot<AuthDataAfterLookup, UserData>(doc => doc.UserData.First());
    var pipeline = new IPipelineStageDefinition[]
                   {
                     lookup,
                     replaceroot,
                   }.Concat(userToIdentityPipeline.Stages);
    foreach (var pipelineStageDefinition in pipeline)
    {
      Console.WriteLine(pipelineStageDefinition.ToString());
    }

    return new PipelineStagePipelineDefinition<AuthData, UserIdentityResult>(pipeline);
  }

  public void AddRoles(IList<RoleData> roles)
  {
    var roleCollection = roleCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    roleCollection.InsertMany(sessionHandle,
                              roles);
  }

  public void AddUsers(IList<UserData> users)
  {
    var userCollection = userCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    userCollection.InsertMany(sessionHandle,
                              users);
  }

  public void AddCertificates(IList<AuthData> certificates)
  {
    var authCollection = authCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    authCollection.InsertMany(sessionHandle, certificates);
  }
}
