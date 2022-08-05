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

using JetBrains.Annotations;

using Microsoft.Extensions.Logging;

using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB;

[PublicAPI]
public class AuthenticationTable : IAuthenticationTable
{
  private readonly ActivitySource                                          activitySource_;
  private readonly MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider_;
  private readonly MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider_;
  private readonly SessionProvider                                         sessionProvider_;
  private readonly MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider_;
  private          PipelineDefinition<AuthData, UserAuthenticationResult>? authToIdentityPipeline_;
  private          bool                                                    isInitialized_;

  private PipelineDefinition<UserData, UserAuthenticationResult>? userToIdentityPipeline_;

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
  public void AddRoles(IEnumerable<RoleData> roles)
  {
    var roleCollection = roleCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    roleCollection.InsertMany(sessionHandle,
                              roles);
  }

  /// <inheritdoc />
  public void AddUsers(IEnumerable<UserData> users)
  {
    var userCollection = userCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    userCollection.InsertMany(sessionHandle,
                              users);
  }

  /// <inheritdoc />
  public void AddCertificates(IEnumerable<AuthData> certificates)
  {
    var authCollection = authCollectionProvider_.Get();
    var sessionHandle  = sessionProvider_.Get();
    authCollection.InsertMany(sessionHandle,
                              certificates);
  }

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

  /// <inheritdoc />
  public async Task<UserAuthenticationResult?> GetIdentityFromUserAsync(string?           id,
                                                                        string?           username,
                                                                        CancellationToken cancellationToken = default)
  {
    using var                        activity       = activitySource_.StartActivity($"{nameof(GetIdentityFromUserAsync)}");
    var                              userCollection = userCollectionProvider_.Get();
    var                              sessionHandle  = sessionProvider_.Get();
    Expression<Func<UserData, bool>> expression;
    // Id matching has priority
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

  /// <summary>
  ///   Gets the user from the given collection by first matching the entry with the matchingFunction and then executing the
  ///   given pipeline
  /// </summary>
  /// <typeparam name="TCollectionDataType">Type of data in the collection</typeparam>
  /// <param name="sessionHandle">Session Handle</param>
  /// <param name="collection">Collection to be used at the start of the pipeline</param>
  /// <param name="pipeline">Pipeline to use</param>
  /// <param name="matchingFunction">Filter to use in front of the pipeline</param>
  /// <param name="cancellationToken">Cancellation token</param>
  /// <returns>
  ///   UserAuthenticationResult object containing the user information, roles and permissions. Null if the user has
  ///   not been found
  /// </returns>
  private static async Task<UserAuthenticationResult?> GetIdentityFromPipelineAsync<TCollectionDataType>(IClientSessionHandle                  sessionHandle,
                                                                                                         IMongoCollection<TCollectionDataType> collection,
                                                                                                         PipelineDefinition<TCollectionDataType,
                                                                                                           UserAuthenticationResult> pipeline,
                                                                                                         Expression<Func<TCollectionDataType, bool>> matchingFunction,
                                                                                                         CancellationToken cancellationToken = default)
  {
    var pipe =
      new PrependedStagePipelineDefinition<TCollectionDataType, TCollectionDataType, UserAuthenticationResult>(PipelineStageDefinitionBuilder.Match(matchingFunction),
                                                                                                               pipeline);

    return await collection.AggregateAsync(sessionHandle,
                                           pipe,
                                           new AggregateOptions(),
                                           cancellationToken)
                           .ContinueWith(task => task.Result.FirstOrDefault(),
                                         cancellationToken)
                           .ConfigureAwait(false);
  }


  /// <summary>
  ///   Gets or generates the pipeline which uses the matched user information to get their roles and permissions
  /// </summary>
  /// <returns>The user to identity pipeline</returns>
  private PipelineDefinition<UserData, UserAuthenticationResult> GetUserToIdentityPipeline()
  {
    if (userToIdentityPipeline_ != null)
    {
      return userToIdentityPipeline_;
    }

    // Get the RoleData for each of the roles of the UserData
    var lookup = PipelineStageDefinitionBuilder.Lookup<UserData, RoleData, UserDataAfterLookup>(roleCollectionProvider_.Get(),
                                                                                                u => u.Roles,
                                                                                                r => r.RoleId,
                                                                                                ual => ual.Roles);
    /* Projects the object into the identity containing:
    - UserId : The database Id of the user
    - UserName : The name of the User
    - Roles : The names of the roles the user has
    - Permissions : The list of permissions, extracted from the roles. Permissions are not repeated
    Equivalent Bson Stage :
    @SONAR-IGNORE-START
    $project: {
        Id: '$_id',
        Username: '$Username',
        Roles: '$Roles.RoleName',
        Permissions: {
            $reduce: {
                input: '$Roles',
                initialValue: [],
                'in': {
                    $setUnion: [
                        '$$value',
                        '$$this.Permissions'
                    ]
                }
            }
        },
        _id: 0
    }
    @SONAR-IGNORE-END
    */
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

  /// <summary>
  ///   Gets or generates the pipeline which uses the matched certificates information to get the corresponding user, their
  ///   role and their permissions
  /// </summary>
  /// <returns>The certificate to identity pipeline</returns>
  private PipelineDefinition<AuthData, UserAuthenticationResult> GetAuthToIdentityPipeline()
  {
    if (authToIdentityPipeline_ != null)
    {
      return authToIdentityPipeline_;
    }

    /*
     When matching, either 1 or 2 certificates can be found. Either the database only has the CN and Fingerprint matching, or there is also a CN only entry.
     When both are present, select the one which matches best (CN AND Fingerprint > CN only).
     First sort by the Fingerprint in descending order (null fingerprint are push to the end)...
    */
    var sortByRelevance = PipelineStageDefinitionBuilder.Sort(new SortDefinitionBuilder<AuthData>().Descending(authData => authData.Fingerprint));
    // ...then limit to 1 result, allowing to keep the best matching.
    var limit = PipelineStageDefinitionBuilder.Limit<AuthData>(1);

    // Get the User corresponding to the UserId from the UserData collection and put it in the UserData field.
    var lookup = PipelineStageDefinitionBuilder.Lookup<AuthData, UserData, AuthDataAfterLookup>(userCollectionProvider_.Get(),
                                                                                                auth => auth.UserId,
                                                                                                user => user.UserId,
                                                                                                authAfterLookup => authAfterLookup.UserData);
    // If the UserId is invalid, the UserData field is an empty array. Stop if this is the case.
    var checkIfValid = PipelineStageDefinitionBuilder.Match<AuthDataAfterLookup>(doc => doc.UserData.Any());
    // Replace the object with the UserData
    var replaceRoot = PipelineStageDefinitionBuilder.ReplaceRoot<AuthDataAfterLookup, UserData>(doc => doc.UserData.First());

    // Use the User to Identity pipeline to create identity from the UserData
    var userToIdentityPipeline = GetUserToIdentityPipeline();
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
}
