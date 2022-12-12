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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Splunk.mdeToSplunkHEC
{
    public static class splunk
    {
        static HttpClient _httpClient = new HttpClient();

        public static string getSourceType(string sourcetype, string resourceId, string category)
        {
            // If this is an AAD sourcetype, append the category to the sourcetype and return
            string[] aadSourcetypes = { Helpers.Utilities.GetEnvironmentVariable("AAD_LOG_SOURCETYPE"), Helpers.Utilities.GetEnvironmentVariable("AAD_NON_INTERACTIVE_SIGNIN_LOG_SOURCETYPE"), Helpers.Utilities.GetEnvironmentVariable("AAD_SERVICE_PRINCIPAL_SIGNIN_LOG_SOURCETYPE"), Helpers.Utilities.GetEnvironmentVariable("AAD_PROVISIONING_LOG_SOURCETYPE") };
            if (aadSourcetypes.Contains(sourcetype))
            {
                return $"{sourcetype}:{category.ToLower()}";
            }

            // Set the sourcetype based on the resourceId
            string sourcetypePattern = "PROVIDERS/(.*?/.*?)(?:/)";
            try
            {
                string st = Regex.Matches(resourceId, sourcetypePattern)[1]
                    .Value.Replace("MICROSOFT.", "azure:")
                    .Replace('.', ':')
                    .Replace('/', ':')
                    .ToLower();
                return $"{st}:{category.ToLower()}";
            }
            catch
            {
                // Could not detrmine the sourcetype from the resourceId
                return sourcetype;
            }
        }

        public static string getEpochTime(string timeString)
        {
            try
            {
                DateTime epochTime;
                DateTime.TryParse(timeString, out epochTime);

                return new DateTimeOffset(epochTime).ToUnixTimeMilliseconds().ToString();
            }
            catch
            {
                return null;
            }
        }

        public static string getTimeStamp(dynamic message) {
            if(message.time != null && string.IsNullOrEmpty(message.time.ToString())) {
                return getEpochTime(message.time.ToString());
                }
            return null;
        }

        public static async Task<int> sendPayloadToHEC(SplunkPayload payload, ILogger log) {
            string hecUrl = Helpers.Utilities.GetEnvironmentVariable("SPLUNK_HEC_URL");
            string hecToken = Helpers.Utilities.GetEnvironmentVariable("SPLUNK_HEC_TOKEN");

            var serializedBody = JsonConvert.SerializeObject(payload);
            log.LogInformation(serializedBody);
            var requestMessage = new HttpRequestMessage(HttpMethod.Post, hecUrl)
            {
                Content = new StringContent(serializedBody, Encoding.UTF8)
            };
            // Add the authorization header
            var authorizationHeader = $"Splunk {hecToken}";
            requestMessage.Headers.Add("Authorization", authorizationHeader);
            requestMessage.Headers.Add("Connection", "keep-alive");
            requestMessage.Headers.Add("Keep-Alive", "timeout=5, max=1000");

            // Post the request
            await _httpClient.SendAsync(requestMessage);
            
            return serializedBody.Length;
        }
    }
}
