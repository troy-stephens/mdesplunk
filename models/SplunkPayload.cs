using System.Collections.Generic;

namespace Splunk.mdeToSplunkHEC
{
    public class SplunkPayload{
        public string sourcetype{get;set;}
        public EventRecords @event{get;set;}

        public SplunkPayload()
        {
            this.@event = new EventRecords();
        }
    }
    public class EventRecords{
        public List<dynamic> records{get;set;}
        public EventRecords()
        {
            this.records = new List<dynamic>();
        }
    }
}