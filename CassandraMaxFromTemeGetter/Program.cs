using Cassandra;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraMaxFromTemeGetter
{
    class Program
    {
        static string cassandraIp = ConfigurationManager.AppSettings["CassandraIp"];
        static string cassandraUserName = ConfigurationManager.AppSettings["CassandraUserName"];
        static string cassandraPassword = ConfigurationManager.AppSettings["CassandraPassword"];
        static int cassandraPort = int.Parse(ConfigurationManager.AppSettings["CassandrPort"]);
        static string MaxTimeStampFile = ConfigurationManager.AppSettings["MaxTimeStampFile"];
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
                if (cassandraUserName != null && cassandraUserName.Length > 0 && cassandraPassword != null && cassandraPassword.Length > 0)
                {
                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp }).WithPort(cassandraPort).WithSocketOptions(options).WithCredentials(cassandraUserName, cassandraPassword).WithQueryTimeout(int.MaxValue).Build();
                }
                else
                {
                    cluster = Cluster.Builder().AddContactPoints(new string[] { cassandraIp }).WithPort(cassandraPort).WithSocketOptions(options).WithQueryTimeout(int.MaxValue).Build();
                }
                File.Delete(MaxTimeStampFile);
                currentSession = cluster.Connect("vegamtagdata");
                Console.WriteLine("Connected to cassandra cluster");

                string getSignaldList = "select distinct signalid,monthyear from tagdatacentral";

                var rowset = currentSession.Execute(getSignaldList);
                //var rowsList = rowset.ToList();
                var maxRows = from r in rowset
                              group r by
               new
               {
                   signalid = (int)r["signalid"]
               }
                into gvs
                              select
                              new
                              {
                                  gvs.Key.signalid,
                                  monthyear = (int)gvs.Max(x => x["monthyear"])

                              };

                string getMaxTimeQry = "select max(fromtime) as maxfromtime from tagdatacentral where signalid= ? and monthyear=?";
                var selectStatement = currentSession.Prepare(getMaxTimeQry);
                using (StreamWriter outputFile = new StreamWriter(MaxTimeStampFile))
                {
                    foreach (var item in maxRows)
                    {
                        int signalid = item.signalid;
                        int monthyear = item.monthyear;

                        var boundStatement = selectStatement.Bind(signalid, monthyear);
                        var maxTimeRowSet = currentSession.Execute(boundStatement);
                        var txt = $"{signalid},{monthyear},{maxTimeRowSet.First()?["maxfromtime"]}";
                         Console.WriteLine(txt);
                        outputFile.WriteLine(txt);
                        //Some comment
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.ReadLine();
        }
    }
}
