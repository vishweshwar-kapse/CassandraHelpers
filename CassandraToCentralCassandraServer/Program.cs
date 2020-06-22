using Cassandra;
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

namespace CassandraToCentralCassandraServer
{
    class Program
    {
        static string cassandraIp = ConfigurationManager.AppSettings["CassandraIp"];
        static string cassandraUserName = ConfigurationManager.AppSettings["CassandraUserName"];
        static string cassandraPassword = ConfigurationManager.AppSettings["CassandraPassword"];
        static int cassandraPort = int.Parse(ConfigurationManager.AppSettings["CassandrPort"]);
        static SignlasInfoBuffer signalsBuffer = new SignlasInfoBuffer(int.Parse(ConfigurationManager.AppSettings["BufferSize"]));
        static string centralUrl = ConfigurationManager.AppSettings["CetntralCassandraServiceURL"];
        static string signalIdListFile = ConfigurationManager.AppSettings["SignalIdListFile"];

        static string SuccessFile = ConfigurationManager.AppSettings["SuccessfullLinesCopiedFile"];
        static string FailedFile = ConfigurationManager.AppSettings["FailedLinesCopiedFile"];
        public static int BufferDelayForSending = int.Parse(ConfigurationManager.AppSettings["BufferDelayForSending"]);

        static ISession currentSession;
        static Cluster cluster;

        static void Main(string[] args)
        {
            try
            {
                SocketOptions options = new SocketOptions();
                options.SetConnectTimeoutMillis(int.MaxValue);
                options.SetReadTimeoutMillis(int.MaxValue);
                options.SetTcpNoDelay(true);
                centralUrl = string.Format("http://{0}", centralUrl);

                if (cassandraUserName != null && cassandraUserName.Length > 0 && cassandraPassword != null && cassandraPassword.Length > 0)
                {
                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp })
                        .WithPort(cassandraPort)
                        .WithCredentials(cassandraUserName, cassandraPassword)
                        .WithSocketOptions(options)
                        .WithQueryTimeout(int.MaxValue).Build();
                }
                else
                {

                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp })
                        .WithPort(cassandraPort)
                        .WithSocketOptions(options)
                        .WithQueryTimeout(int.MaxValue)
                        .Build();
                }

                currentSession = cluster.Connect("vegamtagdata");
                string currentSignalId = "";
                int currentItemCount = 0;
                StringBuilder cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.Append(" select signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate ");
                cqlCommandBuilder.Append(" from tagdatacentral where signalid = #signalid and monthyear in (#monthyear) and fromtime > #starttime and fromtime < #endtime");
                string cqlCommand = cqlCommandBuilder.ToString();
                signalsBuffer.OnBufferLimitReached += On_BufferLimitReached;

                IEnumerable<string> signalIdList = File.ReadLines(signalIdListFile);
                SignalsInfo currentSignal;
                foreach (var line in signalIdList)
                {
                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }
                    string[] items = line.Split(',');
                    currentSignalId = items[0];
                    long.TryParse(items[2], out long FromTime);
                    long.TryParse(items[3], out long SyncEndTime);
                    currentItemCount = 0;
                    var currentCommand = cqlCommand.Replace("#signalid", items[0]);
                    currentCommand = currentCommand.Replace("#monthyear", GetMonthYearBetween(FromTime, SyncEndTime));
                    currentCommand = currentCommand.Replace("#starttime", FromTime.ToString());
                    currentCommand = currentCommand.Replace("#endtime", SyncEndTime.ToString());

                    try
                    {
                        var resultRowSet = currentSession.Execute(currentCommand);
                        foreach (var row in resultRowSet)
                        {
                            currentSignal = new SignalsInfo();
                            currentSignal.ID = Convert.ToInt32(row["signalid"]);
                            currentSignal.MYear = row["monthyear"].ToString();
                            currentSignal.FTime = Convert.ToInt64(row["fromtime"]);
                            currentSignal.TTime = Convert.ToInt64(row["totime"]);
                            currentSignal.Avg = Convert.ToDecimal(row["avg"]);
                            currentSignal.Max = Convert.ToDecimal(row["max"]);
                            currentSignal.Min = Convert.ToDecimal(row["min"]);
                            currentSignal.Data = row["readings"].ToString();
                            currentSignal.CTime = Convert.ToInt64(row["insertdate"]);
                            currentItemCount++;
                            signalsBuffer.AddSignal(currentSignal);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                    Console.WriteLine("Completed pushing data for signalId " + currentSignalId + " with count: " + currentItemCount);
                    File.AppendAllText(SuccessFile, currentSignalId + Environment.NewLine);
                }
                signalsBuffer.FlushData();
                Console.ReadLine();

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task On_BufferLimitReached(SignalsInfo[] buffer)
        {
            List<string> signalsData = new List<string>();
            try
            {

                foreach (var item in buffer)
                {
                    signalsData.Add(JsonConvert.SerializeObject(item));
                }
                await StoreAtCentral(signalsData);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                File.AppendAllText(FailedFile, JsonConvert.SerializeObject(buffer) + Environment.NewLine);
            }
        }

        private static string GetMonthYearBetween(long startTime, long endTime)
        {
            DateTime fromTime = FromUnixTime(startTime), toTime = FromUnixTime(endTime);
            toTime = toTime.AddMonths(1);
            DateTime tempFromTime = fromTime;
            List<string> monthYearList = new List<string>();
            while (tempFromTime <= toTime)
            {
                monthYearList.Add(tempFromTime.ToString("yyyyMM"));
                tempFromTime = tempFromTime.AddMonths(1);
            }
            return string.Join(",", monthYearList.ToArray());
        }
        public static DateTime FromUnixTime(long unixTime)
        {
            return epoch.AddMilliseconds(unixTime);
        }
        private static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static async Task StoreAtCentral(List<string> jsonData)
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

    public delegate Task BufferLimitReached(SignalsInfo[] buffer);

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

    public class SignlasInfoBuffer
    {
        SignalsInfo[] _buffer;
        int _currentIndex = 0;
        int bufferSize = 0;

        public event BufferLimitReached OnBufferLimitReached;

        public SignlasInfoBuffer(int bufferSize)
        {
            _buffer = new SignalsInfo[bufferSize];
            this.bufferSize = bufferSize;
        }

        public void AddSignal(SignalsInfo signal)
        {
            if (_currentIndex >= bufferSize)
            {
                System.Threading.Thread.Sleep(Program.BufferDelayForSending);
                OnBufferLimitReached?.Invoke(_buffer);
                _buffer = new SignalsInfo[bufferSize];
                _currentIndex = 0;
            }
            _buffer[_currentIndex++] = signal;
        }

        public void FlushData()
        {
            var remainingBuffer = _buffer.Where(x => x != null).ToArray();
            if (remainingBuffer.Length > 0)
            {
                OnBufferLimitReached?.Invoke(remainingBuffer);
            }
            Console.WriteLine(" Flushed the data with a buffer count of " + remainingBuffer.Count());
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
