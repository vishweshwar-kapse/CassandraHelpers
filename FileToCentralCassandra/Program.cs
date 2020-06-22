using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace FileToCentralCassandra
{
    class Program
    {
        static string failedAzurePushTags = ConfigurationManager.AppSettings["FailedAzurePushTags"];
        static int lineCountToSkip = int.Parse(ConfigurationManager.AppSettings["LinesToSkip"]);
        static string centralUrl = ConfigurationManager.AppSettings["CetntralCassandraServiceURL"];

        static void Main(string[] args)
        {
            //var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            //XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));some changes..!!

            try
            {
                MainAsync(args).GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {

                throw;
            }
        } 
        static async Task MainAsync(string[] args)
        {
            try
            {
                Console.WriteLine(" Trying to fetch data from the file " + failedAzurePushTags);


                var totalLineCount = File.ReadLines(failedAzurePushTags).LongCount();

                Console.WriteLine("The total number of lines in the file is: " + totalLineCount);
                Console.WriteLine("Press enter if you want to continue");
                Console.ReadLine();

                var messages = File.ReadLines(failedAzurePushTags);
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
                    List<string> messageToBeSent = null;
                    foreach (var item in chunkedList)
                    {
                        messageToBeSent = new List<string>();
                        foreach (var data in item)
                        {
                            messageToBeSent.Add(JsonConvert.SerializeObject(data));
                        }
                        await StoreAtCentralAsync(messageToBeSent);
                        Console.WriteLine("Pushing line number: " + count);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            Console.ReadLine();
        }
        public static async Task StoreAtCentralAsync(List<string> jsonData)
        {
            try
            {

                var chunkedList = jsonData.Chunk(10);

                foreach (var data in chunkedList)
                {
                    using (HttpClientHandler handler = new HttpClientHandler())
                    {
                        handler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
                        using (HttpClient client = new HttpClient(handler, false))
                        {
                            string json = JsonConvert.SerializeObject(data);
                            byte[] jsonBytes = Encoding.UTF8.GetBytes(json);

                            MemoryStream ms = new MemoryStream();
                            using (GZipStream gzip = new GZipStream(ms, CompressionMode.Compress, true))
                            {
                                gzip.Write(jsonBytes, 0, jsonBytes.Length);
                            }
                            ms.Position = 0;
                            StreamContent content = new StreamContent(ms);
                            content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                            content.Headers.ContentEncoding.Add("gzip");
                            HttpResponseMessage respMessage = null;
                            try
                            {
                                if (centralUrl != null)
                                    respMessage = await client.PostAsync(centralUrl + "/storeapi/signaldata/insertdata", content);
                            }
                            catch (Exception ex)
                            {
                                throw;
                            }

                            if ((respMessage == null || respMessage.StatusCode != HttpStatusCode.OK))
                            {
                                throw new Exception("Failed to write to central cassandra");
                            }

                            System.Threading.Thread.Sleep(5);
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
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

    public static class EnumrableChunk
    {
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int chunksize)
        {
            while (source.Any())
            {
                yield return source.Take(chunksize);
                source = source.Skip(chunksize);
            }
        }
    }
}
