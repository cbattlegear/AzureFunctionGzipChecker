using System;
using System.IO;
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

namespace AzureFunctionGzipChecker
{
    public static class VerifyGzipsInFolder
    {
        private static readonly Lazy<TokenCredential> _msiCredential = new Lazy<TokenCredential>(() =>
        {
            // https://docs.microsoft.com/en-us/dotnet/api/azure.identity.defaultazurecredential?view=azure-dotnet
            // Using DefaultAzureCredential allows for local dev by setting environment variables for the current user, provided said user
            // has the necessary credentials to perform the operations the MSI of the Function app needs in order to do its work. Including
            // interactive credentials will allow browser-based login when developing locally.
            return new Azure.Identity.DefaultAzureCredential(includeInteractiveCredentials: true);
        });

        [FunctionName("VerifyGzipsInFolder")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            //Headers for run settings. Storage Account, folder, and container are required
            string storageaccount = req.Headers["x-storage-account"];
            string folder = req.Headers["x-storage-folder"];
            string container = req.Headers["x-storage-container"];
            if(storageaccount == null || folder == null || container == null)
            {
                log.LogError("Function ran without required headers");
                ObjectResult error = new ObjectResult("Missing required headers (x-storage-account, x-storage-folder, or x-storage-container)");
                error.StatusCode = 500;
                return error;
            }

            log.LogInformation("Verifying Gzips in folder " + folder);

            //Optional Headers
            string filesuffix = req.Headers.ContainsKey("x-storage-file-suffix") ? req.Headers["x-storage-file-suffix"].ToString() : ".gz";
            string badfilelistpath = req.Headers.ContainsKey("x-storage-bad-file-list-path") ? req.Headers["x-storage-bad-file-list-path"].ToString() : "gzipissues/currentissues.txt";
            int fullscan = req.Headers.ContainsKey("x-storage-full-scan") ? Convert.ToInt32(req.Headers["x-storage-full-scan"]) : 1;

            BlobServiceClient blobServiceClient = new BlobServiceClient(new Uri($@"https://{storageaccount}.blob.core.windows.net"), _msiCredential.Value);
            BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(container);
            List<BlobItem> blobs = await ListBlobsHierarchicalListing(containerClient, folder, filesuffix, null);

            List<string> bad_files = new List<string>();
            List<GZipResult> gZipResults = new List<GZipResult>();

            foreach (BlobItem item in blobs)
            {
                BlobClient blobClient = containerClient.GetBlobClient(item.Name);
                string full_path = $@"https://{storageaccount}.blob.core.windows.net" + "/" + container + "/" + item.Name;
                using (MemoryStream ms = new MemoryStream())
                {
                    Console.WriteLine(item.Name);
                    await blobClient.DownloadToAsync(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    if (GZipUtilities.IsGZipHeader(ms.ToArray()))
                    {
                        if (fullscan == 1) {
                            if (!(await GZipUtilities.IsGZipValid(ms)))
                            {
                                bad_files.Add(container + "/" + item.Name);
                                gZipResults.Add(new GZipResult { blobPath = full_path, isValid = false });
                                Console.WriteLine("Couldn't Decompress");
                            } else
                            {
                                gZipResults.Add(new GZipResult { blobPath = full_path, isValid = true });
                            }
                        } else
                        {
                            gZipResults.Add(new GZipResult { blobPath = full_path, isValid = true });
                        }
                    }
                    else
                    {
                        bad_files.Add(container + "/" + item.Name);
                        gZipResults.Add(new GZipResult { blobPath = full_path, isValid = false });
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

            return new OkObjectResult(gZipResults);
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
}
