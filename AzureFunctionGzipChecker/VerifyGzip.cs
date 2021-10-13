using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Newtonsoft.Json;

using Azure.Core;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Identity;
using Azure;

namespace AzureFunctionGzipChecker
{
    public static class VerifyGzip
    {
        private static readonly Lazy<TokenCredential> _msiCredential = new Lazy<TokenCredential>(() =>
        {
            // https://docs.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet
            // Using DefaultAzureCredential allows for local dev by setting environment variables for the current user, provided said user
            // has the necessary credentials to perform the operations the MSI of the Function app needs in order to do its work. Including
            // interactive credentials will allow browser-based login when developing locally.
            return new Azure.Identity.DefaultAzureCredential(includeInteractiveCredentials: true);
        });

        [FunctionName("VerifyGzip")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            //Headers for run settings. Storage Account, folder, and container are required
            string storageaccount = req.Headers["x-storage-account"];
            string path = req.Headers["x-storage-path"];
            string container = req.Headers["x-storage-container"];
            if (storageaccount == null || path == null || container == null)
            {
                log.LogError("Function ran without required headers");
                ObjectResult error = new ObjectResult("Missing required headers (x-storage-account, x-storage-folder, or x-storage-container)");
                error.StatusCode = 500;
                return error;
            }

            log.LogInformation("Verifying Gzip: " + path);

            //Optional Headers
            int fullscan = req.Headers.ContainsKey("x-storage-full-scan") ? Convert.ToInt32(req.Headers["x-storage-full-scan"]) : 1;

            BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri($@"https://{storageaccount}.blob.core.windows.net"), _msiCredential.Value);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(container);
            BlobClient blobClient = blobClient = containerClient.GetBlobClient(path); ;

            List<GZipResult> gZipResults = new List<GZipResult>();
            string full_path = $@"https://{storageaccount}.blob.core.windows.net" + "/" + container + "/" + path;
            using (MemoryStream ms = new MemoryStream())
            {
                Console.WriteLine(path);
                try
                {
                    await blobClient.DownloadToAsync(ms);
                }
                catch (RequestFailedException e) when (e.Status == 404)
                {
                    // handle not found error
                    Console.WriteLine("ErrorCode " + e.ErrorCode);
                    return new NotFoundObjectResult("Blob doesn't exist at path: " + full_path);
                }
                ms.Seek(0, SeekOrigin.Begin);
                if (GZipUtilities.IsGZipHeader(ms.ToArray()))
                {
                    if (fullscan == 1)
                    {
                        if (!(await GZipUtilities.IsGZipValid(ms)))
                        {
                            gZipResults.Add(new GZipResult { blobPath = full_path, isValid = false });
                            Console.WriteLine("Couldn't Decompress");
                        }
                        else
                        {
                            gZipResults.Add(new GZipResult { blobPath = full_path, isValid = true });
                        }
                    }
                    else
                    {
                        gZipResults.Add(new GZipResult { blobPath = full_path, isValid = true });
                    }
                }
                else
                {
                    gZipResults.Add(new GZipResult { blobPath = full_path, isValid = false });
                    Console.WriteLine("Bad Header");
                }
            }

            return new OkObjectResult(gZipResults);
        }
    }
}
