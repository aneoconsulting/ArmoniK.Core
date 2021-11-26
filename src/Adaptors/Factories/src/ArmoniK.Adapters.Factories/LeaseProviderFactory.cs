using System;
using ArmoniK.Adapters.MongoDB;
using ArmoniK.Core.Exceptions;
using ArmoniK.Core.Storage;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace ArmoniK.Adapters.Factories
{
    public class LeaseProviderFactory
    {
        private static ILeaseProvider MongoDB(string dbEndpoint, string dbPort, ILoggerFactory loggerFactory)
        {
            MongoClient mongoClient = new MongoClient("mongodb://" + dbEndpoint + ":" + dbPort);
            IMongoDatabase mongoDatabase = mongoClient.GetDatabase("LeaseTable");
            mongoDatabase.CreateCollection("lease", new CreateCollectionOptions<LeaseDataModel>());
            IMongoCollection<LeaseDataModel> leaseCollection = mongoDatabase.GetCollection<LeaseDataModel>("lease");
            TimeSpan acquisitionPeriod = new TimeSpan(seconds: 0, minutes: 1, hours: 0, days: 0);
            TimeSpan acquisitionDuration = new TimeSpan(seconds: 0, minutes: 1, hours: 0, days: 0);
            ILogger<LeaseProvider> logger = loggerFactory.CreateLogger<LeaseProvider>();
            return new LeaseProvider(acquisitionPeriod, acquisitionDuration, leaseCollection,
                mongoClient.StartSession(), logger);
        }

        public static ILeaseProvider Create(string lpService, string lpEndpoint, string lpPort,
            ILoggerFactory loggerFactory)
        {
            if (lpService.Equals("MongoDB"))
            {
                return MongoDB(lpEndpoint, lpPort, loggerFactory);
            }

            throw new NotImplementedException(lpService + " constructor not implemented");
        }

        public static ILeaseProvider CreateFromEnv(ILoggerFactory loggerFactory)
        {
            string lpService = Environment.GetEnvironmentVariable("LEASE_PROVIDER_SERVICE");
            if (lpService == null)
            {
                throw new ArmoniKException("env var LEASE_PROVIDER_SERVICE not found ");
            }

            string lpEndpoint = Environment.GetEnvironmentVariable("LEASE_PROVIDER_ENDPOINT");
            if (lpEndpoint == null)
            {
                throw new ArmoniKException("env var LEASE_PROVIDER_ENDPOINT not found ");
            }

            string lpPort = Environment.GetEnvironmentVariable("LEASE_PROVIDER_PORT");
            if (lpPort == null)
            {
                throw new ArmoniKException("env var LEASE_PROVIDER_PORT not found ");
            }

            return Create(lpService, lpEndpoint, lpPort, loggerFactory);
        }
    }
}
