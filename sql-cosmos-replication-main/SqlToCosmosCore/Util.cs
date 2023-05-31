using System;
using System.Collections.Generic;
using System.Text;

using System.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace SQLtoCosmosDB
{
    public static class Util
    {
		public static string SqlConnectionString = "";
		public static string SqlStartProcedure = "";
		public static string SqlEndProcedure = "";
		public static string CosmosEndpoint = "";
		public static string CosmosKey = "";
		public static string CosmosDatabase = "";
		public static string CosmosContainer = "";
		public static string CosmosPartitionKey = "";

        // Read configuration from appsettings.json
        //     ==> gitignore, add your file to the project
        // PM> install-package Microsoft.Extensions.Configuration
        // PM> install-package Microsoft.Extensions.Configuration.Json   
        // Add appsettings.json to the root folder and set property "Copy to output" as "Copy if newer"
        static Util() {
            IConfigurationBuilder configurationBuilder = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json");
            IConfiguration configuration = configurationBuilder.Build();

            CosmosEndpoint = configuration["CosmosEndpoint"];
            if (CosmosEndpoint == null)
                { CosmosEndpoint = "Hardcoded endpoint goes here"; }

            CosmosKey = configuration["CosmosKey"];
            if (CosmosKey == null)
                { CosmosKey = "Hardcoded Cosmos DB key goes here"; }

            CosmosDatabase = configuration["CosmosDatabase"];
            if (CosmosDatabase == null)
                { CosmosDatabase = "Hardcoded Cosmos DB database goes here"; }

            CosmosContainer = configuration["CosmosContainer"];
            if (CosmosContainer == null)
                { CosmosContainer = "Hardcoded Cosmos DB container goes here"; }

            CosmosPartitionKey = configuration["CosmosPartitionKey"];
            if (CosmosPartitionKey == null)
                { CosmosPartitionKey = "Hardcoded Cosmos DB partition key goes here"; }

            SqlConnectionString = configuration["SqlConnectionString"];
            if (SqlConnectionString == null)
                { SqlConnectionString = "Hardcoded SQL Connection String goes here"; }

            SqlStartProcedure = configuration["SqlStartProcedure"];
            if (SqlStartProcedure == null)
                { SqlStartProcedure = "Hardcoded SQL Start Procedure goes here"; }

            SqlEndProcedure = configuration["SqlEndProcedure"];
            if (SqlEndProcedure == null)
                { SqlEndProcedure = "Hardcoded SQL End Procedure goes here"; }


        }

        // Extension method for SqlDataReader
        public static string SafeGetString(this SqlDataReader reader, int colIndex)
        {
            if (!reader.IsDBNull(colIndex))
                return reader.GetString(colIndex);
            return string.Empty;
        }
    }
}
