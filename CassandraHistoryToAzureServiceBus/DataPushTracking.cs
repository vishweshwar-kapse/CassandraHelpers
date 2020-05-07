using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraHistoryToAzureServiceBus
{
   public class DataPushTracking
    {
        public int SignalId { get; set; }
        public string MonthYear { get; set; }
        public double StartTime { get; set; }
        public double EndTime { get; set; }
        public bool IsDataPushed { get; set; }
        public long NumberOfRecordsPushed { get; set; }
    }
}
