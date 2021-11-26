using System;
using ArmoniK.Adapters.MongoDB;
using ArmoniK.Core.Exceptions;
using ArmoniK.Core.Storage;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;

namespace ArmoniK.Adapters.Factories
{
    public class TableStorageFactory
    {
        private static ITableStorage MongoDB(string db_endpoint, string db_port, string db_table, ILoggerFactory loggerFactory)
        {
            MongoClient mongoClient = new MongoClient("mongodb://" + db_endpoint + ":" + db_port);
            IMongoDatabase mongoDatabase = mongoClient.GetDatabase(db_table);
            mongoDatabase.CreateCollection("session", new CreateCollectionOptions<SessionDataModel>());
            mongoDatabase.CreateCollection("task", new CreateCollectionOptions<TaskDataModel>());
            IMongoCollection<SessionDataModel> sessionCollection = mongoDatabase.GetCollection<SessionDataModel>("session");
            IMongoCollection<TaskDataModel> taskCollection = mongoDatabase.GetCollection<TaskDataModel>("task");
            TimeSpan pollingDelay = new TimeSpan(seconds: 5, minutes: 0, hours: 0, days: 0);
            ILogger<TableStorage> logger = loggerFactory.CreateLogger<TableStorage>();
            return new TableStorage(sessionCollection, taskCollection, mongoClient.StartSession(), pollingDelay, logger);
        }

        public static ITableStorage Create(string db_type, string db_endpoint, string db_port, string db_table,
            ILoggerFactory loggerFactory)
        {
            if (db_type.Equals("MongoDB"))
            {
                return MongoDB(db_endpoint, db_port, db_table, loggerFactory);
            }

            throw new NotImplementedException(db_type + " constructor not implemented");
        }

        public static ITableStorage CreateFromEnv(ILoggerFactory loggerFactory)
        {
            string db_type = Environment.GetEnvironmentVariable("TASKS_STATUS_TABLE_SERVICE");
            if (db_type == null)
            {
                throw new ArmoniKException("env var TASKS_STATUS_TABLE_SERVICE not found ");
            }
            string db_endpoint = Environment.GetEnvironmentVariable("DB_ENDPOINT");
            if (db_endpoint == null)
            {
                throw new ArmoniKException("env var DB_ENDPOINT not found ");
            }
            string db_port = Environment.GetEnvironmentVariable("DB_PORT");
            if (db_port == null)
            {
                throw new ArmoniKException("env var DB_PORT not found ");
            }
            string db_table = Environment.GetEnvironmentVariable("TASKS_STATUS_TABLE_NAME");
            if (db_table == null)
            {
                throw new ArmoniKException("env var TASKS_STATUS_TABLE_NAME not found ");
            }
            return Create(db_type, db_endpoint, db_port, db_table, loggerFactory);
        }
    }
}
