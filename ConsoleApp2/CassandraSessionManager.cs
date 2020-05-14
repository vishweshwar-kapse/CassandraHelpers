using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra;
using log4net;

namespace VegamSignalStoreHandler
{
    internal class CassandraSessionManager
    {
        public bool SessionState { get; set; }
        private static readonly ILog _log = LogManager.GetLogger(typeof(CassandraSessionManager));

        public Cluster cluster = null;
        public ISession currentSession = null;

        public PreparedStatement insertPreparedStmt = null;
        public PreparedStatement failedPreparedStmt = null;
        public PreparedStatement centralPreparedStmt = null;
        public PreparedStatement deletePreparedStmt = null;

        public IStatement BoundInsertStatement = null;

        public bool StartCassandraSession(string[] cassandraServerIPList, string username, string pwd)
        {
            if (cassandraServerIPList.Length < 1)
            {
                _log.Info("M:- StartCassandraSession | V:- no IP address found");
                return false; ;
            }

            if (cluster == null || currentSession == null)
                return StartSession(cassandraServerIPList, username, pwd);
            return true;
        }

        public void StopCassandraSession()
        {
            StopSession();
        }

        private bool StartSession(string[] cassandraServerIPList, string username, string pwd)
        {
            try
            {
                _log.Info("M:- StartSession | V:- starting cassandra session with username:" + username);
                if (username != null && username.Length > 0 && pwd != null && pwd.Length > 0)
                {
                    cluster = Cluster.Builder().AddContactPoints(cassandraServerIPList).WithCredentials(username, pwd).Build();
                }
                else
                {
                    cluster = Cluster.Builder().AddContactPoints(cassandraServerIPList).Build();
                }
                currentSession = cluster.Connect("vegamtagdata");

                StringBuilder cqlCommandBuilder = new StringBuilder();
                cqlCommandBuilder.Append(" insert into tagdata(signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate) ");
                cqlCommandBuilder.Append(" values(?,?,?,?,?,?,?,?,?)");
                insertPreparedStmt = currentSession.Prepare(cqlCommandBuilder.ToString());

                StringBuilder cqlCommandBuilder2 = new StringBuilder();
                cqlCommandBuilder2.Append(" insert into tagdatafailed(signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate) ");
                cqlCommandBuilder2.Append(" values(?,?,?,?,?,?,?,?,?)");
                failedPreparedStmt = currentSession.Prepare(cqlCommandBuilder2.ToString());

                StringBuilder cqlCommandBuilder3 = new StringBuilder();
                cqlCommandBuilder3.Append(" insert into tagdatacentralazurevalidation(signalid, monthyear, fromtime, totime, avg, max, min, readings, insertdate) ");
                cqlCommandBuilder3.Append(" values(?,?,?,?,?,?,?,?,?)");
                centralPreparedStmt = currentSession.Prepare(cqlCommandBuilder3.ToString());

                StringBuilder cqlCommandBuilder4 = new StringBuilder();
                cqlCommandBuilder4.Append(" delete from tagdatafailed where signalid=? and monthyear=? and fromtime=? ");
                deletePreparedStmt = currentSession.Prepare(cqlCommandBuilder4.ToString());

                SessionState = true;

                _log.Info("M:- StartSession | V:- cassandra session started");
                return SessionState;
            }
            catch (Exception ex)
            {
                SessionState = false;
                StopSession();
                _log.Error("M:- StartSession | V:- Not able to start cassandra session", ex);
                return SessionState;
            }
        }

        private void StopSession()
        {
            try
            {
                if (currentSession != null)
                {
                    currentSession.Dispose();
                    currentSession = null;
                }
                if (cluster != null)
                {
                    cluster.Dispose();
                    cluster = null;
                }

                SessionState = false;
            }
            catch (Exception ex)
            {
                SessionState = false;
                _log.Error("M:- StopSession | V:- Error while stopping cassandra message | Ex:- ", ex);
            }
        }

    }
}
