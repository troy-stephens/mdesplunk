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
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Splunk.mdeToSplunkHEC
{
    public static class mdeToSplunkHEC
    {
        const int RECORDS_PER_BATCH = 50;

        [FunctionName("mdeToSplunkHEC")]
        public static async Task Run([EventHubTrigger(
                                            eventHubName: "%EVENTHUB_NAME%",
                                            ConsumerGroup = "%EVENTHUB_CONSUMERGROUP%",
                                            Connection = "EVENTHUB_CONNECTION_STRING")] EventData[] events, ILogger log)
        {
            bool writeToBlob = Helpers.Utilities.GetEnvironmentVariable("WriteToBlob", "false") == "true";
            string blobConnectionString = Helpers.Utilities.GetEnvironmentVariable("BlobConnectionString");
            string containerName = Helpers.Utilities.GetEnvironmentVariable("ContainerName");

            bool expectParsedMessage = true;
            var exceptions = new List<Exception>();
            List<EventData> archiveItems = new List<EventData>();

            var splunkEvent = new SplunkPayload();
            //string sourcetype = Helpers.Utilities.GetEnvironmentVariable("MDE_SOURCETYPE");
            //splunkEvent.sourcetype = sourcetype;

            if (!string.IsNullOrEmpty(Helpers.Utilities.GetEnvironmentVariable("PARSED_MESSAGE")) &&
                string.Equals(Helpers.Utilities.GetEnvironmentVariable("PARSED_MESSAGE"), "false", StringComparison.CurrentCultureIgnoreCase))
                expectParsedMessage = false;

            for (var i = 0; i < events.Length; i++)
            {
                var item = events[i];
                string messageBody = Encoding.UTF8.GetString(item.EventBody);

                dynamic message;
                try
                {
                    log.LogInformation($"Parsing message: {messageBody}");
                    var eventMessages = messageBody.Split("\r\n");
                    log.LogInformation($"Found {eventMessages.Length} events.");
                    for (var j = 0; j < eventMessages.Length; j++)
                    {
                        var eventMessage = eventMessages[j];
                        try
                        {
                            log.LogInformation($"Parsing event message: {eventMessage}");

                            message = JsonConvert.DeserializeObject<dynamic>(eventMessage);

                            if (expectParsedMessage == true)
                            {
                                string eventTimeStamp = splunk.getTimeStamp(message);
                                if (!string.IsNullOrEmpty(eventTimeStamp)) { message.time = eventTimeStamp; }

                                splunkEvent.@event.records.Add(message);
                                if (writeToBlob)
                                {
                                    try
                                    {
                                        var filename = System.Guid.NewGuid().ToString() + ".json";
                                        var serializedBody = JsonConvert.SerializeObject(message);
                                        // write to blob storage
                                        await Helpers.Utilities.WriteToBlob(blobConnectionString, containerName, filename, serializedBody);
                                    }
                                    catch (Exception ex)
                                    {
                                        log.LogError(ex.Message, ex);
                                    }
                                }
                            }
                            else
                            {
                                foreach (dynamic recordEntry in message.records)
                                {
                                    string eventTimeStamp = splunk.getTimeStamp(recordEntry);
                                    if (!string.IsNullOrEmpty(eventTimeStamp)) { recordEntry.time = eventTimeStamp; }

                                    splunkEvent.@event.records.Add(recordEntry);
                                }
                            }

                            if (expectParsedMessage == false || (splunkEvent.@event.records.Count >= RECORDS_PER_BATCH ||
                                (i >= events.Length - 1 && j >= eventMessages.Length - 1))) // last message
                            {
                                await splunk.sendPayloadToHEC(splunkEvent, log);
                                log.LogInformation($"Processed batch of {splunkEvent.@event.records.Count}");
                                splunkEvent.@event.records.Clear();
                            }
                        }
                        catch (JsonReaderException)
                        {
                            try
                            {
                                log.LogWarning($"Invalid json. Parsing message body");
                                string parsedBody = messageBody.Substring(0, messageBody.LastIndexOf("}") + 1);
                                message = JsonConvert.DeserializeObject<dynamic>(parsedBody);
                            }
                            catch (Exception err)
                            {
                                log.LogError($"Error parsing eventhub message: {err}");
                                log.LogError($"{item}");
                                continue;
                            }
                        }
                        catch (Exception err)
                        {
                            log.LogError($"Error parsing eventhub message: {err}");
                            log.LogError($"{item}");
                            continue;
                        }
                    }
                    archiveItems.Add(item);
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
