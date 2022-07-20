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
using ArmoniK.Core.Common.Auth.Authorization;

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Core.Adapters.MongoDB;

public class AuthenticationSource : IAuthenticationSource
{
  private readonly ActivitySource                                          activitySource_;
  private readonly MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider_;
  private readonly MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider_;
  private          bool                                                    isInitialized_;

  private PipelineDefinition<UserData, UserIdentityResult> userToIdentityPipeline_;
  private PipelineDefinition<AuthData, UserIdentityResult> authToIdentityPipeline_;

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

  [ItemCanBeNull]
  private async Task<UserIdentity> GetIdentityFromPipeline<TCollectionDataType>(IClientSessionHandle                                        sessionHandle,
                                                                                IMongoCollection<TCollectionDataType>                       collection,
                                                                                PipelineDefinition<TCollectionDataType, UserIdentityResult> pipeline,
                                                                                Expression<Func<TCollectionDataType, bool>>                 matchingFunctions,
                                                                                CancellationToken                                           cancellationToken)
  {
    var pipe =
      new PrependedStagePipelineDefinition<TCollectionDataType, TCollectionDataType, UserIdentityResult>(PipelineStageDefinitionBuilder.Match(matchingFunctions),
                                                                                                         pipeline);

    var res = await (await collection.AggregateAsync(sessionHandle,
                                                     pipe,
                                                     new AggregateOptions(),
                                                     cancellationToken)
                                     .ConfigureAwait(false)).FirstOrDefaultAsync(cancellationToken)
                                                            .ConfigureAwait(false);

    return res != null
             ? new UserIdentity(res.Id,
                                res.Username,
                                res.Roles,
                                res.Permissions.Select(str => new Permissions.Permission(str)))
             : null;
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
                                         GetAuthToIdentityPipeline(),
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

    return await GetIdentityFromPipeline(sessionHandle,
                                         userCollection,
                                         GetUserToIdentityPipeline(),
                                         user => user.UserId == id,
                                         cancellationToken)
             .ConfigureAwait(false);
  }

  public async Task<UserIdentity> GetIdentityFromNameAsync(string            username,
                                                           CancellationToken cancellationToken)
  {
    using var activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromNameAsync)}");
    var       userCollection = userCollectionProvider_.Get();
    var       sessionHandle  = sessionProvider_.Get();
    return await GetIdentityFromPipeline(sessionHandle,
                                         userCollection,
                                         GetUserToIdentityPipeline(),
                                         user => user.Username == username,
                                         cancellationToken)
             .ConfigureAwait(false);
  }


  private PipelineDefinition<UserData, UserIdentityResult> GetUserToIdentityPipeline()
  {
    if (userToIdentityPipeline_ != null)
      return userToIdentityPipeline_;
    var lookup = PipelineStageDefinitionBuilder.Lookup<UserData, RoleData, UserDataAfterLookup>(roleCollectionProvider_.Get(),
                                                                                                u => u.Roles,
                                                                                                r => r.RoleId,
                                                                                                ual => ual.Roles);
    var projectionStage = PipelineStageDefinitionBuilder.Project<UserDataAfterLookup, UserIdentityResult>(ual => new UserIdentityResult(ual.UserId,
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
    var pipeline = new IPipelineStageDefinition[]
                   {
                     lookup,
                     projectionStage,
                   };
    userToIdentityPipeline_ = new PipelineStagePipelineDefinition<UserData, UserIdentityResult>(pipeline);
    return userToIdentityPipeline_;
  }

  private PipelineDefinition<AuthData, UserIdentityResult> GetAuthToIdentityPipeline()
  {
    if (authToIdentityPipeline_ != null)
      return authToIdentityPipeline_;
    var userToIdentityPipeline = GetUserToIdentityPipeline();
    var lookup = PipelineStageDefinitionBuilder.Lookup<AuthData, UserData, AuthDataAfterLookup>(userCollectionProvider_.Get(),
                                                                                                a => a.UserId,
                                                                                                u => u.UserId,
                                                                                                aal => aal.UserData);
    var checkIfValid = PipelineStageDefinitionBuilder.Match<AuthDataAfterLookup>(doc => doc.UserData.Any());
    var replaceroot  = PipelineStageDefinitionBuilder.ReplaceRoot<AuthDataAfterLookup, UserData>(doc => doc.UserData.First());
    var pipeline = new IPipelineStageDefinition[]
                   {
                     lookup,
                     checkIfValid,
                     replaceroot,
                   }.Concat(userToIdentityPipeline.Stages);
    foreach (var pipelineStageDefinition in pipeline)
    {
      Console.WriteLine(pipelineStageDefinition.ToString());
    }

    authToIdentityPipeline_ = new PipelineStagePipelineDefinition<AuthData, UserIdentityResult>(pipeline);
    return authToIdentityPipeline_;
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
    authCollection.InsertMany(sessionHandle,
                              certificates);
  }
}
