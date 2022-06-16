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
        [FunctionName("mdeToSplunkHEC")]
        public static async Task Run([EventHubTrigger(
                                            eventHubName: "%EVENTHUB_NAME%", 
                                            ConsumerGroup = "%EVENTHUB_CONSUMER_GROUP%", 
                                            Connection = "EVENTHUB_CONNECTION")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();
            List<EventData> archiveItems = new List<EventData>();
            int batchSize = 0;
            int index = 0;
            SplunkPayload splunkEvent = new SplunkPayload();

            foreach (EventData item in events)
            {
                string messageBody = Encoding.UTF8.GetString(item.EventBody);

                dynamic message;
                try
                {
                    log.LogInformation($"Parsing message: {item}");
                    try {
                        message = JsonConvert.DeserializeObject<dynamic>(messageBody);
                    } catch (Exception err) {
                        log.LogError($"Error parsing eventhub message: {err}");
                        log.LogError($"{item}");
                        return;
                    }
                    archiveItems.Add(item);
                    batchSize++;
                    
                    string eventTimeStamp = splunk.getTimeStamp(message);
                    if(!string.IsNullOrEmpty(eventTimeStamp)) { message.time = eventTimeStamp; }

                    splunkEvent.@event.records.Add(message);

                    if(batchSize >= 50 || index >= events.Length - 1)
                    { 
                        try
                        {
                            await splunk.sendPayloadToHEC(splunkEvent, log);
                        }
                        catch(Exception err)
                        {
                            log.LogError($"Error posting to Splunk HTTP Event Collector: {err}");
                            
                            // If the event was not successfully sent to Splunk, drop the event in a storage blob
                            //context.bindings.outputBlob = archiveItems;                       
                        }
                        log.LogInformation($"Processed batch of {batchSize}");
                        archiveItems.Clear();
                        batchSize = 0;
                        
                        splunkEvent.@event.records.Clear();
                    }
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
