/*
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

using Azure.Storage.Blobs;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Splunk.Helpers
{
    internal static class Utilities
    {
        public static string GetEnvironmentVariable(string name, string defaultValue = "")
        {
            string value = System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
            return string.IsNullOrEmpty(value) ? defaultValue : value;
        }

        public static async Task WriteToBlob(string connectionString, string containerName, string blobName, string json)
        {            
            var blobClient = new BlobContainerClient(connectionString, containerName);
            var blob = blobClient.GetBlobClient(blobName);
            await blobClient.CreateIfNotExistsAsync();

            using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
            {
                await blob.UploadAsync(ms);
            }
        }

    }
}
