using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Covid19DataLogger2021
{
    internal class LoggerSettings
    {
        public string DataFolder { get; set; }
        public bool SaveFiles { get; set; }
        public string ConnString { get; set; }
        public SqlConnection conn { get; set; }
        public List<string> IsoCodeList { get; set; }
        public DateTime DayOfSave { get; set; }
        public TimeSpan DaysTimeSpan { get; set; }
        public int DaysBack { get; set; }
        public string SaveDate { get; set; }


    }
}
