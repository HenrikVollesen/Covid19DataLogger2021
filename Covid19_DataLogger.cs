using RestSharp;
using RestSharp.Serialization.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Covid19DataLogger2021
{
    internal class Covid19_DataLogger
    {
        // Just for info: This is the woman who started it all...
        // https://engineering.jhu.edu/case/faculty/lauren-gardner/

        // Fields for navigating in time, in day (24H) steps
        private DateTime DayZero = new DateTime(2020, 1, 22);
        private DateTime DayOfSave = new DateTime();
        private DateTime now = DateTime.Now; // We just need the date of today, not the time in milliseconds, for this app.
        private TimeSpan DaysTimeSpan;
        private TimeSpan Nextday = new TimeSpan(1, 0, 0, 0); // Step precisely 1 day forward
        private TimeSpan Lastday = new TimeSpan(-1, 0, 0, 0); // Step precisely 1 day back
        private int DaysBack;
        private string SaveDate;

        // resource URL for REST API
        private const string ClientString1 = "https://disease.sh/v3/covid-19/historical/";
        private const string ClientString2 = "?lastdays=";

        // Optional to store data files on disk
        private bool StoreDatafiles = false;

        // Base folder for storage of coronavirus data - choose your own folder IF you want to save the data files
        private string DataFolder = @"D:\Data\coronavirus\stats\CountryStats\";

        // read all Country Codes from DimLocation in DB
        private List<string> IsoCodeList = new List<string>();

        // Start properties if no command line arguments
        private string Filename_Stats = "LatestStats_";
        private string DataSourceFile = @"SomeLocalSQLServer\\SomeSQLInstance"; // !!!Remember to change the Server name to your local MSSQL!!!
        private string InitialCatalogFile = "Covid-19_Stats";
        private string UserIDFile = "Covid19_Reader";
        private string PasswordFile = "Corona_2020";
        private string ConnectionString;
        private SqlConnection conn = new SqlConnection();

        // SQL command for getting predefined countries (there should be 185 countries)
        private string GetCountriesCommand = "SELECT Alpha_2_code FROM GetAPICountries()";

        private RestRequest request = null;
        private IRestResponse response_Stats = null;

        private int ConfirmedYesterday = 0;
        private int DeathsYesterday = 0;

        public Covid19_DataLogger()
        {
            SqlConnectionStringBuilder sqlConnectionStringBuilder = new SqlConnectionStringBuilder()
            {
                DataSource = DataSourceFile,
                InitialCatalog = InitialCatalogFile,
                UserID = UserIDFile,
                Password = PasswordFile
            };
            ConnectionString = sqlConnectionStringBuilder.ConnectionString;
        }

        public Covid19_DataLogger(LoggerSettings settings) : this()
        {
            StoreDatafiles = settings.SaveFiles;
            DataFolder = settings.DataFolder;
            ConnectionString = settings.ConnString;
            conn = new SqlConnection(ConnectionString);
        }

        public void ScrapeCovid19Data()
        {
            Thread thread1 = new Thread(GetData);
            thread1.Start();
        }

        private void GetData()
        {
            // In this first block of using the DB connection, we will read the country IsoCodes of the
            // (currently) 185 countries. These IsoCodes are a unique identifier for a country.
            // Example: 'DK' is Denmark
            GetCountryCodes();

            // Next, we will insert any missing days in Dimdate since DayZero. Typically, yesterday must be inserted
            // if the app is run once a day
            InsertDates();

            // Finally, retrieve data per country, starting from the first missing date in that country's collection
            LogData();
        }
        private void SaveStatData(string isoCode, long day_cases, long day_deaths, long day_recovered)
        {
            if (SaveDate.Equals(USDateString(DayZero)))
            {
                ConfirmedYesterday = 0;
                DeathsYesterday = 0;
            }
            else
            {
                DateTime theDate = DayOfSave + Lastday;

                string sel = "SELECT confirmed, deaths " +
                "FROM FactCovid19Stat " +
                "WHERE(date = '" + USDateString(theDate) + "') AND (Alpha_2_code = '" + isoCode + "')";

                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    using (SqlCommand cmd1 = new(sel, connection))
                    {
                        SqlDataReader sqlDataReader = cmd1.ExecuteReader();
                        if (sqlDataReader.Read())
                        {
                            ConfirmedYesterday = sqlDataReader.GetInt32(0);
                            DeathsYesterday = sqlDataReader.GetInt32(1);
                        }
                    }
                }
            }

            long ConfirmedDay = day_cases - ConfirmedYesterday;
            long DeathsDay = day_deaths - DeathsYesterday;

            using (SqlCommand cmd2 = new("Save_DayStat", conn))
            {
                cmd2.CommandType = CommandType.StoredProcedure;
                cmd2.Parameters.AddWithValue("@Alpha_2_code", isoCode);
                cmd2.Parameters.AddWithValue("@date", SaveDate);
                cmd2.Parameters.AddWithValue("@confirmed", day_cases);
                cmd2.Parameters.AddWithValue("@deaths", day_deaths);
                cmd2.Parameters.AddWithValue("@recovered", day_recovered);
                cmd2.Parameters.AddWithValue("@confirmedDay", ConfirmedDay);
                cmd2.Parameters.AddWithValue("@deathsDay", DeathsDay);
                int rowsAffected = cmd2.ExecuteNonQuery();
            }
        }

        private void GetCountryCodes()
        {
            conn.Open();

            using (SqlCommand cmd = new(GetCountriesCommand, conn))
            {
                SqlDataReader isoCodes = cmd.ExecuteReader();

                if (isoCodes.HasRows)
                {
                    while (isoCodes.Read())
                    {
                        string isoCode = isoCodes.GetString(0).Trim();
                        IsoCodeList.Add(isoCode);
                    }
                }
            }
            conn.Close();
        }

        private bool InsertDates()
        {
            bool result = false;
            conn.Open();

            string LastLogDateString = "SELECT TOP 1 date FROM DimDate ORDER BY date DESC";
            DateTime FirstMissingDate;
            TimeSpan nextday = new TimeSpan(1, 0, 0, 0);

            SqlCommand getLastLogDate = new(LastLogDateString, conn);
            getLastLogDate.CommandType = CommandType.Text;
            object o = getLastLogDate.ExecuteScalar();

            if (o == null)
            {
                // The DB seems to be empty. Start from scratch.
                FirstMissingDate = DayZero;
            }
            else if (o is DateTime)
            {
                FirstMissingDate = (DateTime)o + nextday;
            }
            else
            {
                return result;
            }

            DaysTimeSpan = now - FirstMissingDate;
            DaysBack = DaysTimeSpan.Days;
            if (DaysBack > 0) // We will insert days up to yesterday. Probably no data for today anyway
            {
                DateTime theday = FirstMissingDate;
                // Step precisely 1 day forward

                // This loop will run from FirstMissingDate to yesterday, incrementing theDay by 24 hours
                for (int i = DaysBack; i > 0; i--)
                {
                    string SaveDate = USDateString(theday);

                    try
                    {
                        using SqlCommand cmd2 = new("Save_Date", conn);
                        cmd2.CommandType = CommandType.StoredProcedure;
                        cmd2.Parameters.AddWithValue("@date", SaveDate);
                        int rowsAffected = cmd2.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }

                    theday += nextday;
                }
                result = true;
            }

            conn.Close();
            return result;
        }
        private void LogData()
        {
            // variables for reading JSON objects from request
            JsonDeserializer jd;

            dynamic root;
            dynamic timeline;
            dynamic cases;
            dynamic deaths;
            dynamic recovered;
            dynamic day_cases;
            dynamic day_deaths;
            dynamic day_recovered;

            // Start logging data for each country...
            Console.WriteLine("Covid19DataLogger2021 logging...\n");
            foreach (string isoCode in IsoCodeList)
            {
                conn.Open();

                using SqlCommand getLastBadFunc = new("SELECT dbo.FirstBadDate(N'" + isoCode + "')", conn);
                try
                {
                    object o = getLastBadFunc.ExecuteScalar();
                    if (o is DateTime) // IF days are missing, we request data from the first missing date
                    {
                        DateTime LastBadDate = (DateTime)o;
                        DaysTimeSpan = now - LastBadDate;
                        DaysBack = DaysTimeSpan.Days;

                        // Example theURI: https://disease.sh/v3/covid-19/historical/DK/?lastdays=599
                        String theURI = ClientString1 + isoCode + ClientString2 + DaysBack.ToString();
                        RestClient client = new RestClient(theURI);
                        string jsonContents;
                        request = new RestRequest(Method.GET);
                        response_Stats = client.Execute(request);

                        // Storing files is optional 
                        if (StoreDatafiles)
                        {
                            jsonContents = response_Stats.Content;
                            // A unique filename per isoCode is created 
                            string jsonpath = DataFolder + Filename_Stats + isoCode + ".json";
                            Console.WriteLine("Saving file: " + jsonpath);
                            File.WriteAllText(jsonpath, jsonContents);
                            // The country or state datafile was saved
                        }

                        // Here begins parsing of data from the response
                        Console.WriteLine("Parsing request: " + theURI);
                        jd = new JsonDeserializer();
                        root = jd.Deserialize<dynamic>(response_Stats);
                        timeline = root["timeline"];
                        cases = timeline["cases"];
                        deaths = timeline["deaths"];
                        recovered = timeline["recovered"];

                        DayOfSave = LastBadDate;

                        // This loop will run from LastBadDate to yesterday, incrementing theDay by 24 hours
                        for (int i = DaysBack; i > 0; i--)
                        {
                            SaveDate = USDateString(DayOfSave);
                            try
                            {
                                day_cases = cases[SaveDate];
                                day_deaths = deaths[SaveDate];
                                day_recovered = recovered[SaveDate];

                                SaveStatData(isoCode, day_cases, day_deaths, day_recovered);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                            }

                            DayOfSave += Nextday;
                        }
                    }
                    else
                    {
                        Console.WriteLine(isoCode + " OK");
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

                conn.Close();

                // Sometimes the REST Api server will bitch if you try too aggresively to download data - give it a 'rest' tee-hee
                //Thread.Sleep(Delay);
            }
        }


        private static string USDateString(DateTime theDay)
        {
            // A string must be made with the date in US date format:
            // March 04 2021 = "3/4/21"
            // This is because the data are stored in the JSON response as a key–value pair where the key is this date.
            string d = theDay.Day.ToString();
            string m = theDay.Month.ToString();
            string y = theDay.Year.ToString();
            y = y.Substring(2, 2);

            return m + "/" + d + "/" + y;
        }
    }
}
