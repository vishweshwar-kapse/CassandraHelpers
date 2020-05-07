using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.IO;
using System.IO.Compression;
using System.Text.RegularExpressions;

namespace ConsoleApp1
{
    class Program
    {
        static string eventHubName = "hkdctohdf";
        static string connectionString = "Endpoint=sb://hkdctohdf.servicebus.windows.net/;SharedAccessKeyName=HkdcAccessKey;SharedAccessKey=BEuV8DZhsuhUR3WMhowITo+yTWh8LGVXeL3pINs15H8=";
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        static async Task MainAsync(string[] args)
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);

            Random rand = new Random();
            string x1BlahCounterBlahnessInTheCounterISTooBlah = "12312";
            int.TryParse(x1BlahCounterBlahnessInTheCounterISTooBlah, out int ddssds);
            ddssds++;
            ddssds--;
            var er335wasasd334 = ddssds;
            long time = 1585821585789;

            try
            {
                string message = string.Empty;
                SignalsInfo currentSignal = null;
                List<SignalsInfo> signalList = new List<SignalsInfo>();
                int counter = 0;
                for (int i = 0; i < 10; i++)
                {
                    signalList = new List<SignalsInfo>();
                    for (int j = 0; j < 10; j++)
                    {
                        int numberOfSubMesages = rand.Next(40, 150);
                        List<TagData> subMessages = new List<TagData>();
                        TagData currentMessage;
                        for (int k = 0; k < numberOfSubMesages; k++)
                        {
                            currentMessage = new TagData();
                            currentMessage.g = rand.Next(0, 4);
                            currentMessage.q = rand.Next(0, 15);
                            currentMessage.v = (decimal)rand.NextDouble() * 0.24M;
                            currentMessage.t = (DateTime.UtcNow - new DateTime(1970, 1, 1)).TotalMilliseconds;
                            subMessages.Add(currentMessage);
                        }

                        currentSignal = new SignalsInfo
                        {
                            Avg = ++counter,
                            CTime = time += 500,
                            Data = //Regex.Unescape(
                                JsonConvert.SerializeObject(subMessages, Formatting.None)
                                // )
                                ,
                            FTime = time,
                            ID = rand.Next(1, 999),
                            Max = Convert.ToDecimal(rand.NextDouble()),
                            Min = Convert.ToDecimal(rand.NextDouble()),
                            MYear = "202004",
                            TTime = time += 60000
                        };

                        signalList.Add(currentSignal);
                    }

                    message = JsonConvert.SerializeObject(signalList);

                    //Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, message);
                    byte[] mesageBytes = Encoding.UTF8.GetBytes(message);

                    MemoryStream ms = new MemoryStream();

                    using (GZipStream gzip = new GZipStream(ms, CompressionMode.Compress, true))
                    {
                        gzip.Write(mesageBytes, 0, mesageBytes.Length);
                    }
                    ms.Position = 0;
                    File.AppendAllText("Message.txt", message);
                    Console.ForegroundColor = ConsoleColor.Green;
                    eventHubClient.Send(new EventData(ms));
                    Console.WriteLine("Message sent without exception");

                }
                Console.ReadLine();
            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                Console.ResetColor();
                Console.ReadLine();
            }
        }

    }

    public class TagData
    {
        public decimal v { get; set; }
        public int q { get; set; }
        public double t { get; set; }
        public int g { get; set; }
    }

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

    public class SignalsInfo1
    {
        public int ID { get; set; }
        public string MYear { get; set; }
        public long FTime { get; set; }
        public long TTime { get; set; }
        public decimal Max { get; set; }
        public decimal Min { get; set; }
        public decimal Avg { get; set; }
        public List<TagData> Data { get; set; }
        public long CTime { get; set; }

        public SignalsInfo1() { }
    }
}
