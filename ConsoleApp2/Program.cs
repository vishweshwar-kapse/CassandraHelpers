using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using log4net;
using log4net.Config;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Azure.ServiceBus;
using VegamSignalStoreHandler;

namespace ConsoleApp2
{
    class Program
    {
        static string eventHubName = "hkdctohdf";
        static string connectionString = "Endpoint=sb://hkdctohdf.servicebus.windows.net/;SharedAccessKeyName=HkdcAccessKey;SharedAccessKey=BEuV8DZhsuhUR3WMhowITo+yTWh8LGVXeL3pINs15H8=";

        private const string StorageContainerName = "anycontainer";
        private const string StorageAccountName = "vishusstorageaccount";
        private const string StorageAccountKey = "0Age4QEa+RhnVbtH4KSxd2X1jEwaqI2/2AJOlubIxZOltZd3op0Lpf+CrDs4mMXQunpVH4jPAD2h10QsJggcZQ==";

        private static readonly string StorageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);

        public static CassandraDAL dal = new CassandraDAL();

        static void Main(string[] args)
        {
            //var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            //XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));

            try
            {
                MainAsync(args).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {

                throw;
            }
        }

        private static async Task MainAsync(string[] args)
        {
            string[] cassandraDbIp = new string[1];
            cassandraDbIp[0] = "192.168.1.103";


            SimpleEventProcessor.failedTimer.Interval = 5000;
            SimpleEventProcessor.failedTimer.Elapsed += FailedTimer_Elapsed;
            SimpleEventProcessor.failedTimer.Start();

            dal.StartCassandraSession(cassandraDbIp, null, null);

            Console.WriteLine("Initialized cassandra session");

            Console.WriteLine("Registering EventProcessor...");

            var eventProcessorHost = new EventProcessorHost(
                eventHubName,
                PartitionReceiver.DefaultConsumerGroupName,
                connectionString,
                StorageConnectionString,
                StorageContainerName);

            // Registers the Event Processor Host and starts receiving messages
            await eventProcessorHost.RegisterEventProcessorAsync<SimpleEventProcessor>();

            Console.WriteLine("Receiving. Press ENTER to stop worker.");
            Console.ReadLine();

            // Disposes of the Event Processor Host
            await eventProcessorHost.UnregisterEventProcessorAsync();
        }

        private static void FailedTimer_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            if (SimpleEventProcessor.queue.TryDequeue(out List<SignalsInfo> list))
            {
                if (!dal.InsertTagDataCentral(list))
                {
                    SimpleEventProcessor.queue.Enqueue(list);
                    Console.WriteLine("failed to insert to cassandra");

                    string[] cassandraDbIp = new string[1];
                    cassandraDbIp[0] = "192.168.1.103";

                    dal.StartCassandraSession(cassandraDbIp, null, null);
                }
            }
            int totalcountp = 0;
            foreach (var item in SimpleEventProcessor.queue)
            {
                totalcountp += item.Count;
            }
            Console.WriteLine("\r Total signals in the queue is " + totalcountp);
        }
    }
}
