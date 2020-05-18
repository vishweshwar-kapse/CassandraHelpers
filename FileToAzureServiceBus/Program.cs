using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.IO;
using Microsoft.ServiceBus.Messaging;
using System.IO.Compression;
using Newtonsoft.Json;

namespace FileToAzureServiceBus
{
    class Program
    {
        static string eventHubName = ConfigurationManager.AppSettings["EventHubName"];
        static string connectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];

        static string failedCassandraGetTags = ConfigurationManager.AppSettings["FailedTagIds"];
        static string failedAzurePushTags = ConfigurationManager.AppSettings["FailedAzurePushTags"];
        static int lineCountToSkip = int.Parse(ConfigurationManager.AppSettings["LinesToSkip"]);
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine(" Trying to fetch data from the file " + failedAzurePushTags);


                var totalLineCount = File.ReadLines(failedAzurePushTags).LongCount();

                Console.WriteLine("The total number of lines in the file is: " + totalLineCount);
                Console.WriteLine("Press enter if you want to continue");
                Console.ReadLine();

                var messages = File.ReadLines(failedAzurePushTags);

                var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);

                Console.WriteLine(" Connected to the Azure Service bus: " + eventHubName);
                int count = 0;
                foreach (var message in messages)
                {
                    count++;
                    if (count <= lineCountToSkip || string.IsNullOrWhiteSpace(message))
                    {
                        Console.WriteLine("Skipping the line number " + count);
                        continue;
                    }
                    SignalsInfo[] signalData = JsonConvert.DeserializeObject<SignalsInfo[]>(message);
                    var chunkedList = signalData.Split(25);

                    foreach (var item in chunkedList)
                    {
                        var messageString = JsonConvert.SerializeObject(item);
                        byte[] mesageBytes = Encoding.UTF8.GetBytes(messageString);

                        MemoryStream ms = new MemoryStream();

                        using (GZipStream gzip = new GZipStream(ms, CompressionMode.Compress, true))
                        {
                            gzip.Write(mesageBytes, 0, mesageBytes.Length);
                        }
                        ms.Position = 0;
                        Console.WriteLine("Pushing line number: " + count);
                        eventHubClient.Send(new EventData(ms));
                    }

                  
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.WriteLine(" Operation Complete ");
            Console.Read();
        }

        [Serializable]
        public class TagData
        {
            public decimal v { get; set; }
            public int q { get; set; }
            public double t { get; set; }
            public int g { get; set; }
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

    public static class StaticHelper
    {
        public static IEnumerable<IEnumerable<T>> Split<T>(this T[] array, int size)
        {
            for (var i = 0; i < (float)array.Length / size; i++)
            {
                yield return array.Skip(i * size).Take(size);
            }
        }

        public static IEnumerable<IEnumerable<T>> SplitTest<T>(T[] array, int size)
        {
            for (var i = 0; i < (float)array.Length / size; i++)
            {
                yield return array.Skip(i * size).Take(size);
            }
        }
    }
}
