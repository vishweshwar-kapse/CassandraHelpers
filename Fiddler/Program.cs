using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Fiddler
{
    class Program
    {
        static void Main(string[] args)
        {
            long fromtime = 1509430460000;
            long totime = 1589295478090;
            string result = GetMonthYearBetween(fromtime, totime);
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
}
