using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace SQLtoCosmosDB
{
    public class Program
    {
		private static SqlConnection conn = new SqlConnection(Util.SqlConnectionString);
		private static CosmosClient cc = new CosmosClient(Util.CosmosEndpoint, Util.CosmosKey);

		public static async Task Main(string[] args)
		{
            // ASYNC processing
            GenericImporterv2 gi2 = new GenericImporterv2();
            await gi2.CreateCosmosStructure();
            gi2.ProcessStart();
            await gi2.GetItemsForProcessing();
            await gi2.ProcessJSON();
            gi2.ProcessEnd();

            // Lower RUs...
            await gi2.SetRUs(400);
        }

    }
}
