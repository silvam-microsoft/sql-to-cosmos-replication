using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using System.Collections.Concurrent;
using System.Diagnostics;

namespace SQLtoCosmosDB
{
    public class GenericItem {
        public long id;
        public string PartitionKey;
        public string JSONitem;
    }

    public class GenericImporterv2
    {
        private SqlConnection conn = new SqlConnection(Util.SqlConnectionString);
        private CosmosClient cc = new CosmosClient(Util.CosmosEndpoint, Util.CosmosKey);
        private string dbName = Util.CosmosDatabase;
        private string containerName = Util.CosmosContainer;
        private string partitionKey = Util.CosmosPartitionKey;
        private string procGetItems;

        private DateTime startTime;
        private DateTime endTime;

        ConcurrentBag<GenericItem> items;

        public async Task CreateCosmosStructure()
        {
            DatabaseResponse dbResp = await cc.CreateDatabaseIfNotExistsAsync(this.dbName);
            ContainerResponse ctrResp = await dbResp.Database.CreateContainerIfNotExistsAsync(this.containerName, this.partitionKey, 4000);
        }

        public async Task SetRUs(int ruValue) {
            Container c = cc.GetContainer(this.dbName, this.containerName);
            await c.ReplaceThroughputAsync(ruValue);
        }

        public void ProcessStart()
        {
            SqlCommand cmdSPStart = new SqlCommand(Util.SqlStartProcedure, conn);
            cmdSPStart.CommandType = CommandType.StoredProcedure;
            SqlParameter pCollection = new SqlParameter("@Collection", SqlDbType.VarChar, 100);
            cmdSPStart.Parameters.Add(pCollection);
            pCollection.Value = containerName;

            conn.Open();

            SqlDataReader dr = cmdSPStart.ExecuteReader();
            while (dr.Read())
            {
                this.startTime = dr.GetDateTime(0);
                this.endTime = dr.GetDateTime(1);
                this.procGetItems = dr.GetString(2);
            }
            conn.Close();
        }

        public void ProcessEnd()
        {

            SqlCommand cmdSPSEnd = new SqlCommand(Util.SqlEndProcedure, conn);
            cmdSPSEnd.CommandType = CommandType.StoredProcedure;

            SqlParameter pCollection = new SqlParameter("@Collection", SqlDbType.VarChar, 100);
            cmdSPSEnd.Parameters.Add(pCollection);
            pCollection.Value = containerName;

            SqlParameter pMessage = new SqlParameter("@Message", SqlDbType.VarChar, 100);
            cmdSPSEnd.Parameters.Add(pMessage);
            pMessage.Value = "OK";

            SqlParameter pSuccess = new SqlParameter("@success", SqlDbType.Bit);
            cmdSPSEnd.Parameters.Add(pSuccess);
            pSuccess.Value = 1;

            SqlParameter pDocCount = new SqlParameter("@DocumentCount", SqlDbType.Int);
            cmdSPSEnd.Parameters.Add(pDocCount);
            pDocCount.Value = items.Count;

            conn.Open();
            int rows = cmdSPSEnd.ExecuteNonQuery();

            conn.Close();
        }

       

        public async Task GetItemsForProcessing()
        {

            SqlCommand cmdGetIds = new SqlCommand(this.procGetItems, conn);
            cmdGetIds.CommandType = CommandType.StoredProcedure;
            SqlParameter pDateFrom = new SqlParameter("@DateFrom", SqlDbType.DateTime);
            SqlParameter pDateTo = new SqlParameter("@DateTo", SqlDbType.DateTime);
            pDateFrom.Value = this.startTime;
            pDateTo.Value = this.endTime;
            cmdGetIds.Parameters.Add(pDateFrom);
            cmdGetIds.Parameters.Add(pDateTo);

            items = new ConcurrentBag<GenericItem>();
            conn.Open();

            SqlDataReader dr = cmdGetIds.ExecuteReader();
            while (dr.Read())
            {
                items.Add(new GenericItem()
                    { id = dr.GetInt64(0), PartitionKey = dr.SafeGetString(1), JSONitem = dr.SafeGetString(2) }
                );
            }

            conn.Close();
        }

        public async Task ProcessJSON()
        {

            // No items to process, skip it
            if (items.Count == 0)
            {
                Console.WriteLine("No items for processing...");
                return;
            }   

            Stopwatch sw = Stopwatch.StartNew();

            Console.WriteLine("Start processing {0} items - {1}", this.items.Count, DateTime.Now);

            Container c = cc.GetContainer(this.dbName, this.containerName);
            List<Task<ResponseMessage>> lrm = new List<Task<ResponseMessage>>();
            int i = 0;

            foreach (GenericItem gi in this.items)
            {   
                MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(gi.JSONitem));

                if (i % 150 == 0)
                {
                    var resp = await c.UpsertItemStreamAsync(ms, new PartitionKey(gi.PartitionKey));
                    Console.WriteLine("Persisted item id: {0} | pk: {1}", gi.id, gi.PartitionKey);
                }
                else {
                    lrm.Add(c.UpsertItemStreamAsync(ms, new PartitionKey(gi.PartitionKey)));
                }

                i++;
            }

            foreach (Task<ResponseMessage> t in lrm) {
                if (!t.IsCompleted && t.IsFaulted)
                    Console.WriteLine(t.Result.Diagnostics);
            }
            sw.Stop();
            Console.WriteLine("Finished processing {0} items - {1} - Time: {2}", this.items.Count, DateTime.Now, sw.Elapsed.TotalSeconds);

            // 1000 RUs / 30 to await
            // Finished processing 19972 items - 8/11/2022 12:38:44 PM - Time: 162.3688514

            // 5000 RUs / 150 to await
            // Finished processing 19972 items - 8/11/2022 12:46:51 PM - Time: 73.5125151

            // 4000 RUs / 120 to await
            // Finished processing 19972 items - 8 / 11 / 2022 2:08:57 PM - Time: 71.4156413
        }
    }
}
