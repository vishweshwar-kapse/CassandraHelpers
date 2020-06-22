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

namespace CassandraToCassandraTagDataCopy
{
    class Program
    {
        static string eventHubName = ConfigurationManager.AppSettings["EventHubName"];
        static string connectionString = ConfigurationManager.AppSettings["Microsoft.ServiceBus.ConnectionString"];

        static string SourceCassandraIp = ConfigurationManager.AppSettings["SourceCassandraIp"];
        static string SourceCassandraUserName = ConfigurationManager.AppSettings["SourceCassandraUserName"];
        static string SourceCassandraPassword = ConfigurationManager.AppSettings["SourceCassandraPassword"];
        static int SourceCassandraPort = int.Parse(ConfigurationManager.AppSettings["SourceCassandrPort"]);

        static string DestinationCassandraIp = ConfigurationManager.AppSettings["DestinationCassandraIp"];
        static string DestinationCassandraUserName = ConfigurationManager.AppSettings["DestinationCassandraUserName"];
        static string DestinationCassandraPassword = ConfigurationManager.AppSettings["DestinationCassandraPassword"];
        static int DestinationCassandraPort = int.Parse(ConfigurationManager.AppSettings["DestinationCassandrPort"]);

        static string TagListFileName = ConfigurationManager.AppSettings["TagListFileName"];

        static string SuccessFile = ConfigurationManager.AppSettings["SuccessfullLinesCopiedFile"];
        static string FailedCassandraInsert = ConfigurationManager.AppSettings["FailedCassandraInsert"];

        static string FailedAzurePushTags = ConfigurationManager.AppSettings["FailedAzurePushTags"];

        static SignlasInfoBuffer signalsBuffer = new SignlasInfoBuffer(int.Parse(ConfigurationManager.AppSettings["BufferSize"]));

        static ISession SourceCurrentSession;
        static Cluster SourceCluster;

        static ISession DestinationCurrentSession;
        static Cluster DestinationCluster;

        public static PreparedStatement insertPreparedStmt = null;

        static EventHubClient eventHubClient;

        static void Main(string[] args)
        {
            try
            {
                SocketOptions options = new SocketOptions();
                options.SetConnectTimeoutMillis(int.MaxValue);
                options.SetReadTimeoutMillis(int.MaxValue);
                options.SetTcpNoDelay(true);

                if (SourceCassandraUserName != null && SourceCassandraUserName.Length > 0 && SourceCassandraPassword != null && SourceCassandraPassword.Length > 0)
                {
                    SourceCluster = Cluster.Builder().AddContactPoints(new string[] { SourceCassandraIp })
                        .WithPort(SourceCassandraPort)
                        .WithCredentials(SourceCassandraUserName, SourceCassandraPassword)
                        .WithSocketOptions(options)
                        .WithQueryTimeout(int.MaxValue).Build();
                }
                else
                {

                    SourceCluster = Cluster.Builder().AddContactPoints(new string[] { SourceCassandraIp })
                        .WithPort(SourceCassandraPort)
                        .WithSocketOptions(options)
                        .WithQueryTimeout(int.MaxValue)
                        .Build();
                }
                SourceCurrentSession = SourceCluster.Connect("vegamtagdata");
                Console.WriteLine("Connected to Source Cassandra");

                if (DestinationCassandraUserName != null && DestinationCassandraUserName.Length > 0 && DestinationCassandraPassword != null && DestinationCassandraPassword.Length > 0)
                {
                    DestinationCluster = Cluster.Builder().AddContactPoints(new string[] { DestinationCassandraIp })
                        .WithPort(DestinationCassandraPort)
                        .WithCredentials(DestinationCassandraUserName, DestinationCassandraPassword)
                        .WithSocketOptions(options)
                        .WithQueryTimeout(int.MaxValue).Build();
                }
                else
                {

                    DestinationCluster = Cluster.Builder().AddContactPoints(new string[] { DestinationCassandraIp })
                        .WithPort(DestinationCassandraPort)
                        .WithSocketOptions(options)
                        .WithQueryTimeout(int.MaxValue)
                        .Build();
                }

                DestinationCurrentSession = DestinationCluster.Connect("vegamtagdata");

                eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);

                StringBuilder cqlCommandBuilder1 = new StringBuilder();
                cqlCommandBuilder1.Append(" insert into tagdatacentral (signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate) ");
                cqlCommandBuilder1.Append(" values(?,?,?,?,?,?,?,?,?)");
                insertPreparedStmt = DestinationCurrentSession.Prepare(cqlCommandBuilder1.ToString());

                Console.WriteLine("Connected to Destination Cassandra");
                string currentSignalId = "";
                int currentItemCount = 0;
                StringBuilder cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.Append(" select signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate ");
                cqlCommandBuilder.Append(" from tagdatacentral where signalid = #signalid and monthyear = #monthyear and fromtime > #starttime and fromtime < #endtime");
                string cqlCommand = cqlCommandBuilder.ToString();

                signalsBuffer.OnBufferLimitReached += On_BufferLimitReached;
                signalsBuffer.OnBufferLimitReached += SendDataToAzure;

                IEnumerable<string> signalIdList = File.ReadLines(TagListFileName);
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
                    currentCommand = currentCommand.Replace("#monthyear", items[1]);
                    currentCommand = currentCommand.Replace("#starttime", FromTime.ToString());
                    currentCommand = currentCommand.Replace("#endtime", SyncEndTime.ToString());


                    try
                    {
                        var resultRowSet = SourceCurrentSession.Execute(currentCommand);
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


            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static Task On_BufferLimitReached(SignalsInfo[] buffer)
        {
            InsertTagData(buffer);
            return null;
        }

        private static async Task SendDataToAzure(SignalsInfo[] buffer)
        {
            string message = "";
            try
            {
                var tempBuffer = buffer.Chunk(10);

                foreach (var item in tempBuffer)
                {
                    message = JsonConvert.SerializeObject(item);

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
            }
            catch (Exception)
            {
                File.AppendAllText(FailedAzurePushTags, message + Environment.NewLine);
            }
        }

        public static void InsertTagData(IEnumerable<SignalsInfo> signals)
        {
            try
            {

                var batchStm = new BatchStatement();
                foreach (SignalsInfo signal in signals)
                {
                    batchStm.Add(insertPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Avg, signal.Max, signal.Min, signal.Data, signal.CTime));
                }
                var temo = DestinationCurrentSession.Execute(batchStm);

            }
            catch (Exception)
            {
                File.AppendAllText(FailedCassandraInsert, JsonConvert.SerializeObject(signals) + Environment.NewLine);
                Console.WriteLine("Failed to write to cassandra");
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
                //System.Threading.Thread.Sleep(Program.BufferDelayForSending);
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
