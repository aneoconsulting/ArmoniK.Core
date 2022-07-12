using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Core.Adapters.MongoDB.Common;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel;
using ArmoniK.Core.Adapters.MongoDB.Table.DataModel.Auth;
using ArmoniK.Core.Common;
using ArmoniK.Core.Common.Auth.Authentication;
using ArmoniK.Core.Common.Storage;

using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;

namespace ArmoniK.Core.Adapters.MongoDB
{
  public class AuthenticationSource : IAuthenticationSource
  {
    private          bool                                                    isInitialized_;
    private readonly ActivitySource                                          activitySource_;
    private readonly SessionProvider                                         sessionProvider_;
    private readonly MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider_;
    private readonly MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider_;
    private readonly MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider_;

    public AuthenticationSource(SessionProvider                                         sessionProvider,
                                MongoCollectionProvider<UserData, UserDataModelMapping> userCollectionProvider,
                                MongoCollectionProvider<AuthData, AuthDataModelMapping> authCollectionProvider,
                                MongoCollectionProvider<RoleData, RoleDataModelMapping> roleCollectionProvider,
                                ILogger<AuthenticationSource>                                      logger,
                                ActivitySource                                          activitySource)
    {
      sessionProvider_        = sessionProvider;
      userCollectionProvider_ = userCollectionProvider;
      roleCollectionProvider_ = roleCollectionProvider;
      authCollectionProvider_ = authCollectionProvider;

      Logger                  = logger;
      activitySource_         = activitySource;

      
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

    public async Task<ClaimsIdentity> GetIdentityAsync(string            cn,
                                                       string            fingerprint,
                                                       CancellationToken cancellationToken)
    {
      using var      activity       = activitySource_.StartActivity($"{nameof(GetIdentityAsync)}");
      var            authCollection = authCollectionProvider_.Get();
      var            sessionHandle  = sessionProvider_.Get();
      /*
       * db.AuthData.aggregate([
  {
    $match: {
      "CN": "dylan.brasseur",
      "Fingerprint": "testdylan"
    }
  },
  {
    $lookup: {
      from: "UserData",
      localField: "UserId",
      foreignField: "UserId",
      as: "userData"
    }
  },
  {
    $lookup: {
      from: "RoleData",
      localField: "userData.Roles",
      foreignField: "RoleName",
      as: "Roles"
    }
  },
  {
    $project: {
      "UserId": 1,
      "CN": 1,
      "Fingerprint": 1,
      "Roles": {
        $first: "$userData.Roles"
      },
      "Permissions": {
        $reduce: {
          "input": "$Roles.Permissions",
          "initialValue": [],
          "in": {
            "$setUnion": [
              "$$this",
              "$$value"
            ]
          }
        }
      }
    }
  }
])
       */
      authCollection.AsQueryable(sessionHandle);
      //PipelineDefinition<AuthData, UserPermissions> pipeline    = new EmptyPipelineDefinition<UserPermissions>();
      //var                                           permissions = await authCollection.AggregateAsync(sessionHandle, pipeline);
    }

    public async Task<ClaimsIdentity> GetIdentityFromIdAsync(string            id,
                                                             CancellationToken cancellationToken)
      => throw new NotImplementedException();

    public async Task<ClaimsIdentity> GetIdentityFromNameAsync(string            username,
                                                         CancellationToken cancellationToken)
      => throw new NotImplementedException();
  }
}
