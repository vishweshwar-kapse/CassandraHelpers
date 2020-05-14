using Cassandra;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraHistoryToAzureServiceBus
{
    class Program
    {
        static string eventHubName = ConfigurationManager.AppSettings["EventHubName"];
        static string connectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];
        static string signalIdListFile = ConfigurationManager.AppSettings["SignalIdListFile"];
        static string failedCassandraGetTags = ConfigurationManager.AppSettings["FailedTagIds"];
        static string failedAzurePushTags = ConfigurationManager.AppSettings["FailedAzurePushTags"];

        static string cassandraIp = ConfigurationManager.AppSettings["CassandraIp"];
        static string cassandraUserName = ConfigurationManager.AppSettings["CassandraUserName"];
        static string cassandraPassword = ConfigurationManager.AppSettings["CassandraPassword"];
        static int cassandraPort = int.Parse(ConfigurationManager.AppSettings["CassandrPort"]);
        static long SyncEndTime = long.Parse(ConfigurationManager.AppSettings["SyncEndDate"]);
        static string logFile = "LogFile";
        static ISession currentSession;
        static Cluster cluster;
        static SignlasInfoBuffer signalsBuffer = new SignlasInfoBuffer(int.Parse(ConfigurationManager.AppSettings["BufferSize"]));
        static EventHubClient eventHubClient;
        static void Main(string[] args)
        {
            bool isSuccess = false;
            try
            {
                SocketOptions options = new SocketOptions();
                options.SetConnectTimeoutMillis(int.MaxValue);
                options.SetReadTimeoutMillis(int.MaxValue);
                options.SetTcpNoDelay(true);

                File.AppendAllText(logFile, "Started pushing data at  " + DateTime.Now);

                if (cassandraUserName != null && cassandraUserName.Length > 0 && cassandraPassword != null && cassandraPassword.Length > 0)
                {
                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp }).WithPort(cassandraPort).WithCredentials(cassandraUserName, cassandraPassword).WithSocketOptions(options).WithQueryTimeout(int.MaxValue).Build();
                }
                else
                {

                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp }).WithPort(cassandraPort).WithSocketOptions(options).WithQueryTimeout(int.MaxValue).Build();
                }

                currentSession = cluster.Connect("vegamtagdata");

                eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);

                StringBuilder cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.Append(" select signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate ");
                cqlCommandBuilder.Append(" from tagdatacentral where signalid = #signalid and monthyear in (#monthyear) and fromtime > #starttime and fromtime < #endtime");
                string cqlCommand = cqlCommandBuilder.ToString();
                signalsBuffer.OnBufferLimitReached += SendDataToAzure;

                IEnumerable<string> signalIdList = File.ReadLines(signalIdListFile);
                SignalsInfo currentSignal;
                int count = 1;
                foreach (var line in signalIdList)
                {
                    Console.WriteLine("Currently processing the line number: " + count++);
                    File.AppendAllText(logFile, "Currently processing the line" + line);
                    string[] items = line.Split(',');

                    isSuccess = long.TryParse(items[2], out long FromTime);

                    if (!isSuccess)
                    {
                        File.AppendAllText(failedCassandraGetTags, line + Environment.NewLine);
                        continue;
                    }
                    var currentCommand = cqlCommand.Replace("#signalid", items[0]);
                    currentCommand = currentCommand.Replace("#monthyear", GetMonthYearBetween(FromTime, SyncEndTime));
                    currentCommand = currentCommand.Replace("#starttime", FromTime.ToString());
                    currentCommand = currentCommand.Replace("#endtime", SyncEndTime.ToString());
                    try
                    {
                        var resultRowSet = currentSession.Execute(currentCommand);
                        var itemcount = 0;
                        //Just some more code changes
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
                            signalsBuffer.AddSignal(currentSignal);
                            itemcount++;
                        }
                        Console.WriteLine("Finished processing the line number: " + count + " With a total item count of:" + itemcount);
                        File.AppendAllText(logFile, "Finished processing the line" + line + " With a total item count of:" + itemcount);
                    }
                    catch (Exception)
                    {
                        File.AppendAllText(failedCassandraGetTags, line + Environment.NewLine);
                        continue;
                    }
                }
                signalsBuffer.FlushData();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task SendDataToAzure(SignalsInfo[] buffer)
        {
            string message = "";
            try
            {
                message = JsonConvert.SerializeObject(buffer);

                //Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, message);
                byte[] mesageBytes = Encoding.UTF8.GetBytes(message);

                MemoryStream ms = new MemoryStream();

                using (GZipStream gzip = new GZipStream(ms, CompressionMode.Compress, true))
                {
                    gzip.Write(mesageBytes, 0, mesageBytes.Length);
                }
                ms.Position = 0;
                await eventHubClient.SendAsync(new EventData(ms));
                Console.WriteLine("Sent a batch of data to Azure");
            }
            catch (Exception ex)
            {
                File.AppendAllText(failedAzurePushTags, message + Environment.NewLine);
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

    public delegate Task BufferLimitReached(SignalsInfo[] buffer);

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
}
