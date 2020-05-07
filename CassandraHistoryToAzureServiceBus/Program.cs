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
        static double SyncEndTime = double.Parse(ConfigurationManager.AppSettings["SyncEndDate"]);
        static ISession currentSession;
        static Cluster cluster;
        static SignlasInfoBuffer signalsBuffer = new SignlasInfoBuffer(5);
        static EventHubClient eventHubClient;
        static void Main(string[] args)
        {
            bool isSuccess = false;
            try
            {
                if (cassandraUserName != null && cassandraUserName.Length > 0 && cassandraPassword != null && cassandraPassword.Length > 0)
                {
                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp }).WithPort(cassandraPort).WithCredentials(cassandraUserName, cassandraPassword).WithQueryTimeout(int.MaxValue).Build();
                }
                else
                {
                    
                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp }).WithPort(cassandraPort).WithQueryTimeout(int.MaxValue).Build();
                }

                currentSession = cluster.Connect("vegamtagdata");

                eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);

                StringBuilder cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.Append(" select signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate ");
                cqlCommandBuilder.Append(" from tagdatacentral where signalid = ? and monthyear = ? and fromtime >= ? and fromtime < ?");
                var selectStatement = currentSession.Prepare(cqlCommandBuilder.ToString());

                signalsBuffer.OnBufferLimitReached += SendDataToAzure;

                string[] signalIdList = File.ReadAllLines(signalIdListFile);
                SignalsInfo currentSignal;

                foreach (var line in signalIdList)
                {
                    string[] items = line.Split(',');

                    isSuccess = int.TryParse(items[0], out int SignalId);
                    isSuccess &= int.TryParse(items[1], out int MonthYear);
                    isSuccess &= long.TryParse(items[2], out long FromTime);

                    if (!isSuccess)
                    {
                        File.AppendAllText(failedCassandraGetTags, line + Environment.NewLine);
                        continue;
                    }
                    var boundStatement = selectStatement.Bind(SignalId, MonthYear, FromTime, SyncEndTime).SetPageSize(50);
                    try
                    {
                        var resultRowSet = currentSession.Execute(boundStatement);

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
                            //jkjlkjkljlkj kadjsdlkaslk kadmklamsd
                            signalsBuffer.AddSignal(currentSignal);
                        }

                    }
                    catch (Exception)
                    {
                        File.AppendAllText(failedCassandraGetTags, line + Environment.NewLine);
                        continue;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void SendDataToAzure(SignalsInfo[] buffer)
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
                eventHubClient.Send(new EventData(ms));
            }
            catch (Exception ex)
            {
                File.AppendAllText(failedAzurePushTags, message + Environment.NewLine);
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

    public delegate void BufferLimitReached(SignalsInfo[] buffer);

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
    }
}
