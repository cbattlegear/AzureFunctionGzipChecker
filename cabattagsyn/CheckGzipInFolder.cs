using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Identity;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure;
using System.Collections.Generic;
using Azure.Core;

namespace cabattagsyn
{
    public static class CheckGzipInFolder
    {
        private static readonly Lazy<TokenCredential> _msiCredential = new Lazy<TokenCredential>(() =>
        {
            // https://docs.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet
            // Using DefaultAzureCredential allows for local dev by setting environment variables for the current user, provided said user
            // has the necessary credentials to perform the operations the MSI of the Function app needs in order to do its work. Including
            // interactive credentials will allow browser-based login when developing locally.
            return new Azure.Identity.DefaultAzureCredential(includeInteractiveCredentials: true);
        });

        [FunctionName("CheckGzipInFolder")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            //Headers for run settings. Storage Account, folder, and container are required
            string storageaccount = req.Headers["x-storage-account"];
            string folder = req.Headers["x-storage-folder"];
            string container = req.Headers["x-storage-container"];
            if(storageaccount == null || folder == null || container == null)
            {
                ObjectResult error = new ObjectResult("Missing required headers (x-storage-account, x-storage-folder, or x-storage-container)");
                error.StatusCode = 500;
                return error;
            }

            //Optional Headers
            string filesuffix = req.Headers.ContainsKey("x-storage-file-suffix") ? req.Headers["x-storage-file-suffix"].ToString() : ".gz";
            string badfilelistpath = req.Headers.ContainsKey("x-storage-bad-file-list-path") ? req.Headers["x-storage-bad-file-list-path"].ToString() : "gzipissues/currentissues.txt";
            int fullscan = req.Headers.ContainsKey("x-storage-full-scan") ? Convert.ToInt32(req.Headers["x-storage-full-scan"]) : 1;

            //string folder = req.Query["folder"];
            //string container = req.Query["container"];

            BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri($@"https://{storageaccount}.blob.core.windows.net"), _msiCredential.Value);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(container);
            List<BlobItem> blobs = await ListBlobsHierarchicalListing(containerClient, folder, filesuffix, null);

            List<string> bad_files = new List<string>();

            foreach (BlobItem item in blobs)
            {
                BlobClient blobClient = containerClient.GetBlobClient(item.Name);
                using (MemoryStream ms = new MemoryStream())
                {
                    Console.WriteLine(item.Name);
                    await blobClient.DownloadToAsync(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    if (GZipTool.IsGZipHeader(ms.ToArray()))
                    {
                        if (fullscan == 1) {
                            if (!(await GZipTool.IsGZipValid(ms)))
                            {
                                bad_files.Add(container + "/" + item.Name);
                                Console.WriteLine("Couldn't Decompress");
                            }
                        }
                    }
                    else
                    {
                        bad_files.Add(container + "/" + item.Name);
                        Console.WriteLine("Bad Header");
                    }
                }

            }

            using (Stream s = new MemoryStream())
            {
                using (StreamWriter bad_files_contents = new StreamWriter(s))
                {
                    bad_files_contents.Write(String.Join("\n", bad_files.ToArray()));

                    BlobClient bad_file_blob = containerClient.GetBlobClient(badfilelistpath);
                    await bad_file_blob.UploadAsync(s, overwrite: true);
                }
            }

            return new OkObjectResult($"Found {bad_files.Count} corrupt GZips");
        }
#nullable enable
        private static async Task<List<BlobItem>> ListBlobsHierarchicalListing(BlobContainerClient container,
                                                       string? prefix,
                                                       string? filesuffix,
                                                       int? segmentSize)
        {
            List<BlobItem> list = new List<BlobItem>();
            try
            {
                // Call the listing operation and return pages of the specified size.
                var resultSegment = container.GetBlobsByHierarchyAsync(prefix: prefix, delimiter: "/")
                    .AsPages(default, segmentSize);

                // Enumerate the blobs returned for each page.
                await foreach (Azure.Page<BlobHierarchyItem> blobPage in resultSegment)
                {
                    // A hierarchical listing may return both virtual directories and blobs.
                    foreach (BlobHierarchyItem blobhierarchyItem in blobPage.Values)
                    {
                        if (!blobhierarchyItem.IsPrefix)
                        {
                            if (filesuffix != null && blobhierarchyItem.Blob.Name.EndsWith(filesuffix))
                            {
                                list.Add(blobhierarchyItem.Blob);
                            }
                        }
                    }
                }
                return list;
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine(e.Message);
                Console.ReadLine();
                throw;
            }
        }
    }
#nullable restore

    /// <summary>
    /// GZIP utility methods.
    /// </summary>
    public static class GZipTool
    {
        /// <summary>
        /// Checks the first two bytes in a GZIP file, which must be 31 and 139.
        /// </summary>
        public static bool IsGZipHeader(byte[] arr)
        {
            return arr.Length >= 2 &&
                arr[0] == 31 &&
                arr[1] == 139;
        }

        public static async Task<bool> IsGZipValid(MemoryStream ms)
        {
            try
            {
                using GZipStream decomp = new GZipStream(ms, CompressionMode.Decompress);
                int bytes_read = 0;
                byte[] temp_data = new byte[1024];
                do
                {
                   bytes_read = await decomp.ReadAsync(temp_data, 0, 1024);
                } while (bytes_read == 1024);

                return true;
            }
            catch
            {
                return false;
            }
        }
    }
}
