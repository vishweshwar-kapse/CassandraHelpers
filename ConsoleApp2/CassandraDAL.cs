using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using log4net;
using ConsoleApp2;

namespace VegamSignalStoreHandler
{
    public class CassandraDAL
    {
        private static CassandraSessionManager cassandraSessionMgr = null;
        private static string[] ipAddressList = null;
        private static readonly ILog log = LogManager.GetLogger(typeof(CassandraDAL));
        public bool ConnectionState { get { return cassandraSessionMgr.SessionState; } set { } }

        public CassandraDAL()
        {
            cassandraSessionMgr = new CassandraSessionManager();
        }

        public bool StartCassandraSession(string[] cassandraServerIPList, string username, string pwd)
        {
            try
            {
                if (cassandraSessionMgr != null)
                {
                    cassandraSessionMgr = new CassandraSessionManager();
                }

                if (cassandraServerIPList.Any())
                    ipAddressList = cassandraServerIPList;

                if (!ipAddressList.Any())
                {
                    log.Info("M:- StartCassandraSession | V:- No Cassandra Server IP Found");
                    return false;
                }

                cassandraSessionMgr.StartCassandraSession(ipAddressList, username, pwd);
            }
            catch (Exception ex)
            {
                log.Error("M:- StartCassandraSession | V:- Error while Starting Cassandra Session | Ex:- ", ex);
            }

            return ConnectionState;
        }

        public void StopCassandraSession()
        {
            cassandraSessionMgr.StopCassandraSession();
        }

        #region Getting Sync Failed Records
        public RowSet GetFailedTagDataList()
        {
            try
            {
                StringBuilder cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.Append("select distinct signalid, monthyear from tagdatafailed");

                if (ConnectionState)
                    return cassandraSessionMgr.currentSession.Execute(cqlCommandBuilder.ToString());
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- GetFailedTagDataList | V:- cassandra db no host available | Ex:- ", ex);
                return null;
            }
            catch (Exception ex)
            {
                log.Error("M:- GetFailedTagDataList | V:- error while reading distinct failed records | Ex:- ", ex);
                return null;
            }
        }

        public RowSet GetFailedTagData(int signalID, int monthYear, int noOfRecords)
        {
            try
            {
                for (; ; )
                {

                }
                StringBuilder cqlCommand = new StringBuilder();
                cqlCommand.AppendFormat("select * from tagdatafailed where signalid={0} and monthyear={1} order by fromtime limit {2} ", signalID, monthYear, noOfRecords);
                if (ConnectionState)
                    return cassandraSessionMgr.currentSession.Execute(cqlCommand.ToString());
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("Cassandra Session Down");
                }
               
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- GetFailedTagData | V:- cassandra db no host available | Ex:- ", ex);
                return null;
            }
            catch (Exception ex)
            {
                log.Error("M:- GetFailedTagData | V:- error while readaing failed records | Ex:- ", ex);
                return null;
            }
        }
        #endregion

        #region Remove Failed Records after sync
        public bool RemoveFailedTagData(IEnumerable<SignalsInfo> signals)
        {
            var batchStm = new BatchStatement();
            RowSet rs = null;
            try
            {
                foreach (SignalsInfo signal in signals)
                {
                    batchStm.Add(cassandraSessionMgr.deletePreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime));
                }

                if (ConnectionState)
                    rs = cassandraSessionMgr.currentSession.Execute(batchStm);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- RemoveFailedTagData | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- RemoveFailedTagData | V:- cassandra db no host available | Ex:- ", ex);
                return false;
            }
            catch (Exception ex)
            {
                log.Error("M:- RemoveFailedTagData | V:- error deleting batch [failed data] | Ex:- ", ex);
                return false;
            }

            if (rs != null)
                return true;
            else
                return false;
        }

        public bool RemoveFailedTagData(SignalsInfo signal)
        {
            RowSet rs = null;
            try
            {
                BoundStatement insertBS = cassandraSessionMgr.deletePreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime);

                if (ConnectionState)
                    rs = cassandraSessionMgr.currentSession.Execute(insertBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- RemoveFailedTagData | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- RemoveFailedTagData | V:- cassandra db no host available | Ex:- ", ex);
                return false;
            }
            catch (Exception ex)
            {
                log.Error("M:- RemoveFailedTagData | V:- error deleting batch [failed data] | Ex:- ", ex);
                return false;
            }

            if (rs != null)
                return true;
            else
                return false;
        }
        #endregion

        #region Local cassandra insert statement
        public void InsertTagData(IEnumerable<SignalsInfo> signals)
        {
            try
            {
                var batchStm = new BatchStatement();
                foreach (SignalsInfo signal in signals)
                {
                    batchStm.Add(cassandraSessionMgr.insertPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Avg, signal.Max, signal.Min, signal.Data, signal.CTime));
                }

                if (ConnectionState)
                    cassandraSessionMgr.currentSession.Execute(batchStm);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- InsertTagData | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagData | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagData | V:- error while inserting local records | Ex:- ", ex);
            }
        }

        public void InsertTagData(int signalID, string monthYear, long fromtime, long totime, decimal maxReading, decimal minReading, decimal avgReading, string jsonReadings, long currTime)
        {
            try
            {
                BoundStatement insertBS = cassandraSessionMgr.insertPreparedStmt.Bind(signalID, Convert.ToInt32(monthYear), fromtime, totime, avgReading, maxReading, minReading, jsonReadings);

                if (ConnectionState)
                    cassandraSessionMgr.currentSession.Execute(insertBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:-InsertTagData | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagData | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagData | V:- error while inserting local records | Ex:- ", ex);
            }
        }

        public void InsertTagData(SignalsInfo signal)
        {
            try
            {
                BoundStatement insertBS = cassandraSessionMgr.insertPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Avg, signal.Max, signal.Min, signal.Data, signal.CTime);

                if (ConnectionState)
                    cassandraSessionMgr.currentSession.Execute(insertBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- InsertTagData | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagData | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagData | V:- error cassandra insert | Ex:- ", ex);
            }
        }

        #endregion

        #region Central cassandra insert statements
        public bool InsertTagDataCentral(IEnumerable<SignalsInfo> signals)
        {
            var batchStmt = new BatchStatement();
            batchStmt.SetBatchType(BatchType.Logged);

            RowSet rs = null;
            try
            {
                foreach (var signal in signals)
                {
                    batchStmt.Add(cassandraSessionMgr.centralPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Avg, signal.Max, signal.Min, signal.Data, signal.CTime));
                }

                if (ConnectionState)
                    rs = cassandraSessionMgr.currentSession.Execute(batchStmt);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    var cassandraDbIp = new string[1];
                    cassandraDbIp[0] = "192.168.1.102";

                    Program.dal.StartCassandraSession(cassandraDbIp, null, null);
                    log.Error("M:-InsertTagDataCentral | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                var cassandraDbIp = new string[1];
                cassandraDbIp[0] = "192.168.1.102";

                Program.dal.StartCassandraSession(cassandraDbIp, null, null);
                log.Error("M:- InsertTagDataCentral | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagDataCentral | V:- error batch update | Ex:- ", ex);
                var cassandraDbIp = new string[1];
                cassandraDbIp[0] = "192.168.1.102";

                Program.dal.StartCassandraSession(cassandraDbIp, null, null);
                // return true;
            }

            return rs != null;
        }

        public RowSet InsertTagDataCentral(int signalID, string monthYear, long fromtime, long totime, decimal maxReading, decimal minReading, decimal avgReading, string jsonReadings, long currTime)
        {
            try
            {
                BoundStatement insertBS = cassandraSessionMgr.centralPreparedStmt.Bind(signalID, Convert.ToInt32(monthYear), fromtime, totime, maxReading, minReading, avgReading, jsonReadings, currTime);

                if (ConnectionState)
                    return cassandraSessionMgr.currentSession.Execute(insertBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- InsertTagDataCentral | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagDataCentral | V:- cassandra db no host available | Ex:- ", ex);
                return null;
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagDataCentral | V:- error while inserting central records | Ex:- ", ex);
                return null;
            }
        }

        public RowSet InsertTagDataCentral(SignalsInfo signal)
        {
            try
            {
                var insertBS = cassandraSessionMgr.
                    centralPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Max, signal.Min, signal.Avg, signal.Data, signal.CTime);

                if (ConnectionState)
                    return cassandraSessionMgr.currentSession.Execute(insertBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    return null;
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagDataCentral | V:- cassandra db no host available | Ex:- ", ex);
                return null;
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagDataCentral | V:- error while inserting central records | Ex:- ", ex);
                return null;
            }
        }
        #endregion

        #region Central cassandra failed statements
        public void InsertTagDataFailed(int signalID, string monthYear, long fromtime, long totime, decimal maxReading, decimal minReading, decimal avgReading, string jsonReadings, long currTime)
        {
            try
            {
                var failedBS = cassandraSessionMgr.failedPreparedStmt.Bind(signalID, Convert.ToInt32(monthYear), fromtime, totime, maxReading, minReading, avgReading, jsonReadings, currTime);

                if (ConnectionState)
                    cassandraSessionMgr.currentSession.Execute(failedBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- InsertTagDataFailed | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagDataFailed | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagDataFailed | V:- error while inserting failed records | Ex:- ", ex);
            }
        }

        public void InsertTagDataFailed(IEnumerable<SignalsInfo> signals)
        {
            try
            {
                var batchStm = new BatchStatement();
                foreach (var signal in signals)
                {
                    batchStm.Add(cassandraSessionMgr.failedPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Avg, signal.Max, signal.Min, signal.Data, signal.CTime));
                }

                if (ConnectionState)
                    cassandraSessionMgr.currentSession.Execute(batchStm);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:- InsertTagDataFailed | V:- Cassandra Session Down");
                }
            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagDataFailed | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagDataFailed | V:- error while inserting failed records |Ex:- ", ex);
            }
        }

        public void InsertTagDataFailed(SignalsInfo signal)
        {
            try
            {
                var insertBS = cassandraSessionMgr.
                    failedPreparedStmt.Bind(signal.ID, Convert.ToInt32(signal.MYear), signal.FTime, signal.TTime, signal.Avg, signal.Max, signal.Min, signal.Data, signal.CTime);

                if (ConnectionState)
                    cassandraSessionMgr.currentSession.Execute(insertBS);
                else
                {
                    cassandraSessionMgr.StopCassandraSession();
                    throw new Exception("M:-InsertTagDataFailed | V:- Cassandra Session Down");
                }

            }
            catch (Cassandra.NoHostAvailableException ex)
            {
                cassandraSessionMgr.StopCassandraSession();
                log.Error("M:- InsertTagDataFailed | V:- cassandra db no host available | Ex:- ", ex);
            }
            catch (Exception ex)
            {
                log.Error("M:- InsertTagDataFailed | V:- error while inserting failed records | Ex:- ", ex);
            }
        }
        #endregion

        #region || Get History Data from tagdataCentral ||

        public static RowSet GetHistoricalData(int signalId, long fromTime, long toTime, string monthYearList, bool byMin)
        {
            try
            {
                var cqlCommandBuilder = new StringBuilder();
                if (byMin)
                    cqlCommandBuilder.AppendFormat("select avg, fromtime from tagdatacentral where signalid = {0} and monthyear in ({1}) and fromtime >= {2} and fromtime <= {3} ", signalId, monthYearList, fromTime, toTime);
                else
                    cqlCommandBuilder.AppendFormat("select readings from tagdatacentral where signalid = {0} and monthyear in ({1}) and fromtime >= {2} and fromtime <= {3} ", signalId, monthYearList, fromTime, toTime);
                return cassandraSessionMgr.currentSession.Execute(cqlCommandBuilder.ToString());
            }
            catch (Exception ex)
            {
                log.Error("Error while reading historical data : ", ex);
                throw;
            }
        }

        public static RowSet GetPreviousDateTimeData(int signalId, long fromTime, int monthYear, bool byMin)
        {
            try
            {
                var cqlCommandBuilder = new StringBuilder();
                if (byMin)
                    cqlCommandBuilder.AppendFormat(" select avg, fromtime from tagdatacentral where signalid = {0} and monthyear = {1} and fromtime <= {2} order by fromtime desc limit 1 ", signalId, monthYear, fromTime);
                else
                    cqlCommandBuilder.AppendFormat(" select readings from tagdatacentral where signalid = {0} and monthyear = {1} and fromtime <= {2} order by fromtime desc limit 1 ", signalId, monthYear, fromTime);
                return cassandraSessionMgr.currentSession.Execute(cqlCommandBuilder.ToString());
            }
            catch (Exception ex)
            {
                log.Error("Error while reading records of previous datetime : ", ex);
                throw;
            }
        }

        public static RowSet CountRecord(int signalId, long fromTime, int monthYear)
        {
            try
            {
                var cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.AppendFormat(" select count(*) from tagdatacentral where signalid = {0} and monthyear = {1} and fromtime <= {2} ", signalId, monthYear, fromTime);
                return cassandraSessionMgr.currentSession.Execute(cqlCommandBuilder.ToString());
            }
            catch (Exception ex)
            {
                log.Error("Error while reading record count : ", ex);
                throw;
            }
        }

        public static int GetNumberOfRecordsForSignalInDuration(int signalId, long fromTime, long toTime, string monthYearList)
        {
            try
            {
                var cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.AppendFormat(" select count(*) from tagdatacentral where signalid = {0} and monthyear in({1})  and fromtime >= {2} and fromtime <= {3} ", signalId, monthYearList, fromTime, toTime);
                var result = cassandraSessionMgr.currentSession.Execute(cqlCommandBuilder.ToString());
                if (result == null)
                    return 0;
                var firstRow = result.FirstOrDefault();
                if (firstRow == null)
                    return 0;
                return Convert.ToInt32(firstRow["count"]);
            }
            catch (Exception ex)
            {
                log.Error("Error while reading record count : ", ex);
                throw;
            }
        }

        #endregion
    }
}
