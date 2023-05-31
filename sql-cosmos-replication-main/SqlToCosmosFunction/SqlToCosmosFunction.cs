using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace SqlToCosmosFunction
{
    public class SqlToCosmosFunction
    {
        [FunctionName("ReplicateSqlToCosmos")]
        public async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req, ILogger log)
        {
            log.LogInformation($"C# HTTP trigger function executed at: {DateTime.Now}");
            await SQLtoCosmosDB.Program.Main(new string[0]);
            return new OkObjectResult("Success");
        }
    }
}

