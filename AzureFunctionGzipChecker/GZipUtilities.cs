using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;

namespace AzureFunctionGzipChecker
{
    /// <summary>
    /// GZIP utility methods.
    /// </summary>
    public static class GZipUtilities
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
