# Azure Function for GZip Verification

Azure function that directly connects to an Azure Blob Storage account
and verifies the health of GZip compressed files. This can be done individually
or a folder at a time.

## Setup

Deploy the code to a consumption based Azure Function (or any Azure Function
 app you have ready to go). 

[Add a System Assigned Managed Identity to your Azure Function App](https://docs.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=dotnet#add-a-system-assigned-identity)

Give the Managed Identity the Storage Blob Contributor permission

Now you should be fully set up to access your blob storage from your function.
You can add these permissions to multiple Storage Accounts if you need to access
more than one.

## Usage

Once deployed you will have two separate functions you can call. Arguments
are passed via headers for compatibility with Azure Data Factory.

### VerifyGzip

#### Required Headers
**x-storage-account** The Azure Blob Storage Account name

**x-storage-container** The container within the provided Storage Account

**x-storage-path** The path within the container to the specific blob you 
would like to verify

#### Optional Headers
**x-storage-full-scan** 1 or 0 (Defaults to 1), 1 does a full decompression to
verify the Gzip'd archive. 0 only does a header check *(often just checking the 
header is enough)*

#### Output
JSON formatted result set showing full blob path and if it is a verified archive.

```
[
  {
    "blobPath": "https://account.blob.core.windows.net/container/BadGzips/bad.json.gz",
    "isValid": false
  }
]
```

### VerifyGzipsinFolder

#### Required Headers
**x-storage-account** The Azure Blob Storage Account name

**x-storage-container** The container within the provided Storage Account

**x-storage-folder** The path within the container to the folder you would like
to scan. Trailing slash required.

#### Optional Headers
**x-storage-full-scan** 1 or 0 (Defaults to 1), 1 does a full decompression to
verify the Gzip'd archive. 0 only does a header check *(often just checking the 
header is enough)*

**x-storage-file-suffix** (Defaults to .gz) The file extension or suffix you
would like to limit your scan to.

**x-storage-bad-file-list-path** (Defaults to gzipissues/currentissues.txt) The 
file path under your container to store a line delimited list of corrupt Gzip'd
archives. 

#### Output
JSON formatted result set showing full blob path and if it is a verified archive.

```
[
  {
    "blobPath": "https://account.blob.core.windows.net/container/BadGzips/bad.json.gz",
    "isValid": false
  },
  {
    "blobPath": "https://account.blob.core.windows.net/container/GoodGzips/good.json.gz",
    "isValid": true
  }
]
```

And a line delimited file is output to your storage account listing all corrupt
archives. This is usuable as a file list in a copy activity in Azure Data Factory.