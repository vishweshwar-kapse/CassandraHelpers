using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using System.Threading.Tasks;
using System.IO.Compression;
using System.IO;
using Newtonsoft.Json;
using System.Collections.Concurrent;

namespace ConsoleApp2
{
    public class SimpleEventProcessor : IEventProcessor
    {
        public static ConcurrentQueue<List<SignalsInfo>> queue = new ConcurrentQueue<List<SignalsInfo>>();

        public static System.Timers.Timer failedTimer = new System.Timers.Timer();

        public Task CloseAsync(PartitionContext context, CloseReason reason)
        {
            Console.WriteLine($"Processor Shutting Down. Partition '{context.PartitionId}', Reason: '{reason}'.");
            return Task.CompletedTask;
        }

        public Task OpenAsync(PartitionContext context)
        {
            Console.WriteLine($"SimpleEventProcessor initialized. Partition: '{context.PartitionId}'");
            return Task.CompletedTask;
        }

        public Task ProcessErrorAsync(PartitionContext context, Exception error)
        {
            Console.WriteLine($"Error on Partition: {context.PartitionId}, Error: {error.Message}");
            return Task.CompletedTask;
        }

        public Task ProcessEventsAsync(PartitionContext context, IEnumerable<EventData> messages)
        {
            foreach (var eventData in messages)
            {
                var data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
                MemoryStream ms = new MemoryStream(eventData.Body.Array);

                using (GZipStream gzipStream = new GZipStream(ms, CompressionMode.Decompress))
                {
                    using (StreamReader messageData = new StreamReader(gzipStream, Encoding.UTF8))
                    {
                        try
                        {
                            string result = messageData.ReadToEnd();
                            var deserializedData = JsonConvert.DeserializeObject<List<SignalsInfo>>(result);

                            List<SignalsInfo> list = new List<SignalsInfo>();



                            bool isInserted = Program.dal.InsertTagDataCentral(deserializedData);
                            if (!isInserted)
                            {
                                queue.Enqueue(deserializedData);
                            }

                            //File.AppendAllText("Message.txt", result);
                            Console.WriteLine("\r got some data at" + DateTime.Now.ToString("yyyyMMdd HHmmss.zzz"));
                        }
                        catch (Exception ex)
                        {
                        }
                    }
                }
            }

            return context.CheckpointAsync();
        }
    }
    [Serializable]
    public class SignalsInfo
    {
        public int ID { get; set; }
        public string MYear { get; set; }
        public long FTime { get; set; }
        public long TTime { get; set; }
        public decimal Max { get; set; }
        public decimal Min { get; set; }
        public decimal Avg { get; set; }
        public string Data { get; set; }
        public long CTime { get; set; }

        public SignalsInfo() { }
    }
}
