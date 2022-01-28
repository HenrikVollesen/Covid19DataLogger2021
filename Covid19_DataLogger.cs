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
        private readonly DateTime DayZero = new(2020, 1, 22);
        private readonly DateTime now = DateTime.Now; // We just need the date of today, not the time in milliseconds, for this app.
        private readonly TimeSpan NextDay = new(1, 0, 0, 0); // Step precisely 1 day forward
        private readonly TimeSpan PreviousDay = new(-1, 0, 0, 0); // Step precisely 1 day back

        // resource URL for REST API
        private const string ClientString1 = "https://disease.sh/v3/covid-19/historical/";
        private const string ClientString2 = "?lastdays=";

        // Optional to store data files on disk
        private bool StoreDatafiles = false;

        // Base folder for storage of coronavirus data - choose your own folder IF you want to save the data files
        private string DataFolder = @"D:\Data\coronavirus\stats\CountryStats";

        private static List<SqlConnectionStringBuilder> ConnectionStrings = new List<SqlConnectionStringBuilder>();

        // SQL command for getting predefined countries (there should be 185 countries)
        private string GetCountriesCommand = "SELECT Alpha_2_code FROM GetAPICountries()";

        private RestRequest request = null;
        private IRestResponse response_Stats = null;

        private int ConfirmedYesterday = 0;
        private int DeathsYesterday = 0;

        public void Log(string settings)
        {
            ParseSettings(settings);

            // data may be logged to more than DB, therefore the foreach loop

            int i = 0;

#if DEBUG

            foreach (SqlConnectionStringBuilder s in ConnectionStrings)
            {
                if (i > 0)
                break;
                LoggerSettings loggerSettings = new LoggerSettings()
                {
                    SaveFiles = StoreDatafiles,
                    DataFolder = DataFolder,
                    ConnString = s.ConnectionString
                };

                Thread thread1 = new Thread(GetData);
                thread1.Start(loggerSettings);
                StoreDatafiles = false; // Only store the first time
                i++;
            }
#else

            foreach (SqlConnectionStringBuilder s in ConnectionStrings)
            {
                LoggerSettings loggerSettings = new LoggerSettings()
                {
                    SaveFiles = false,
                    DataFolder = "",
                    ConnString = s.ConnectionString
                };

                Thread thread1 = new Thread(GetData);
                thread1.Start(loggerSettings);
                StoreDatafiles = false; // Only store the first time
            }
#endif
        }

        private void GetData(object ls)
        {
            LoggerSettings loggerSettings = (LoggerSettings)ls;

            // Initialize objects before using them in methods
            loggerSettings.conn = new SqlConnection(loggerSettings.ConnString);
            loggerSettings.IsoCodeList = new List<string>();
            loggerSettings.DayOfSave = new();
            loggerSettings.DaysBack = new();

            // In this first block of using the DB connection, we will read the country IsoCodes of the
            // (currently) 185 countries. These IsoCodes are a unique identifier for a country.
            // Example: 'DK' is Denmark
            GetCountryCodes(loggerSettings);

            // Next, we will insert any missing days in Dimdate since DayZero. Typically, yesterday must be inserted
            // if the app is run once a day
            InsertDates(loggerSettings);

            // Finally, retrieve data per country, starting from the first missing date in that country's collection
            LogData(loggerSettings);
        }
        private void GetCountryCodes(LoggerSettings ls)
        {
            ls.conn.Open();

            using (SqlCommand cmd = new(GetCountriesCommand, ls.conn))
            {
                SqlDataReader isoCodes = cmd.ExecuteReader();

                if (isoCodes.HasRows)
                {
                    while (isoCodes.Read())
                    {
                        string isoCode = isoCodes.GetString(0).Trim();
                        ls.IsoCodeList.Add(isoCode);
                    }
                }
            }
            ls.conn.Close();
        }

        private bool InsertDates(LoggerSettings ls)
        {
            bool result = false;
            ls.conn.Open();

            string LastLogDateString = "SELECT TOP 1 date FROM DimDate ORDER BY date DESC";
            DateTime FirstMissingDate;

            SqlCommand getLastLogDate = new(LastLogDateString, ls.conn);
            getLastLogDate.CommandType = CommandType.Text;
            object o = getLastLogDate.ExecuteScalar();

            if (o == null)
            {
                // The DB seems to be empty. Start from scratch.
                FirstMissingDate = DayZero;
            }
            else if (o is DateTime)
            {
                FirstMissingDate = (DateTime)o + NextDay;
            }
            else
            {
                return result;
            }

            ls.DaysTimeSpan = now - FirstMissingDate;
            ls.DaysBack = ls.DaysTimeSpan.Days;
            if (ls.DaysBack > 0) // We will insert days up to yesterday. Probably no data for today anyway
            {
                DateTime theday = FirstMissingDate;
                // Step precisely 1 day forward

                // This loop will run from FirstMissingDate to yesterday, incrementing theDay by 24 hours
                for (int i = ls.DaysBack; i > 0; i--)
                {
                    string SaveDate = USDateString(theday);

                    try
                    {
                        using SqlCommand cmd2 = new("Save_Date", ls.conn);
                        cmd2.CommandType = CommandType.StoredProcedure;
                        cmd2.Parameters.AddWithValue("@date", SaveDate);
                        int rowsAffected = cmd2.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }

                    theday += NextDay;
                }
                result = true;
            }

            ls.conn.Close();
            return result;
        }
        private void LogData(LoggerSettings ls)
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
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(ls.ConnString); // Only used for printout
            string DB = "; Database " + builder.DataSource;

            foreach (string isoCode in ls.IsoCodeList)
            {
                ls.conn.Open();

                using SqlCommand getLastBadFunc = new("SELECT dbo.FirstBadDate(N'" + isoCode + "')", ls.conn);
                try
                {
                    object o = getLastBadFunc.ExecuteScalar();
                    if (o is DateTime) // IF days are missing, we request data from the first missing date
                    {
                        DateTime LastBadDate = (DateTime)o;
                        ls.DaysTimeSpan = now - LastBadDate;
                        ls.DaysBack = ls.DaysTimeSpan.Days;

                        int DaysBack;

                        // If we should save the files, we will go all the way back to day zero
                        if (ls.SaveFiles)
                        {
                            TimeSpan DaysBackToZero = now - DayZero;
                            DaysBack = DaysBackToZero.Days;
                        }
                        else
                        {
                            DaysBack = ls.DaysBack;
                        }

                        // Example theURI: https://disease.sh/v3/covid-19/historical/DK/?lastdays=599
                        // string theURI = ClientString1 + isoCode + ClientString2 + ls.DaysBack.ToString();
                        string theURI = ClientString1 + isoCode + ClientString2 + DaysBack.ToString();
                        RestClient client = new RestClient(theURI);
                        string jsonContents;
                        request = new RestRequest(Method.GET);
                        response_Stats = client.Execute(request);

                        // Storing files is optional 
                        if (ls.SaveFiles)
                        {
                            jsonContents = response_Stats.Content;
                            // A unique filename per isoCode is created 
                            string jsonpath = DataFolder + @"\" + isoCode + ".json";
                            Console.WriteLine("Saving file: " + jsonpath);
                            File.WriteAllText(jsonpath, jsonContents);
                            // The country or state datafile was saved
                        }

                        // Here begins parsing of data from the response
                        Console.WriteLine("Saving data for: " + isoCode + DB );
                        jd = new JsonDeserializer();
                        root = jd.Deserialize<dynamic>(response_Stats);
                        timeline = root["timeline"];
                        cases = timeline["cases"];
                        deaths = timeline["deaths"];
                        recovered = timeline["recovered"];

                        ls.DayOfSave = LastBadDate;

                        // This loop will run from LastBadDate to yesterday, incrementing theDay by 24 hours
                        for (int i = ls.DaysBack; i > 0; i--)
                        {
                            ls.SaveDate = USDateString(ls.DayOfSave);
                            try
                            {
                                day_cases = cases[ls.SaveDate];
                                day_deaths = deaths[ls.SaveDate];
                                day_recovered = recovered[ls.SaveDate];

                                SaveStatData(ls, isoCode, day_cases, day_deaths, day_recovered);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e.Message);
                            }

                            ls.DayOfSave += NextDay;
                        }
                    }
                    else
                    {
                        Console.WriteLine("Data OK for " + isoCode + DB);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }

                ls.conn.Close();

                // Sometimes the REST Api server will bitch if you try too aggresively to download data - give it a 'rest' tee-hee
                //Thread.Sleep(Delay);
            }
        }

        private void SaveStatData(LoggerSettings ls, string isoCode, long day_cases, long day_deaths, long day_recovered)
        {
            if (ls.SaveDate.Equals(USDateString(DayZero)))
            {
                ConfirmedYesterday = 0;
                DeathsYesterday = 0;
            }
            else
            {
                DateTime theDate = ls.DayOfSave + PreviousDay; // Get data from the day before

                string sel = "SELECT confirmed, deaths " +
                "FROM FactCovid19Stat " +
                "WHERE(date = '" + USDateString(theDate) + "') AND (Alpha_2_code = '" + isoCode + "')";

                using (SqlConnection connection = new SqlConnection(ls.ConnString))
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

            using (SqlCommand cmd2 = new("Save_DayStat", ls.conn))
            {
                cmd2.CommandType = CommandType.StoredProcedure;
                cmd2.Parameters.AddWithValue("@Alpha_2_code", isoCode);
                cmd2.Parameters.AddWithValue("@date", ls.SaveDate);
                cmd2.Parameters.AddWithValue("@confirmed", day_cases);
                cmd2.Parameters.AddWithValue("@deaths", day_deaths);
                cmd2.Parameters.AddWithValue("@recovered", day_recovered);
                cmd2.Parameters.AddWithValue("@confirmedDay", ConfirmedDay);
                cmd2.Parameters.AddWithValue("@deathsDay", DeathsDay);
                int rowsAffected = cmd2.ExecuteNonQuery();
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

        private void ParseSettings(string settings)
        {
            IRestResponse Settings;
            JsonDeserializer jd;
            dynamic dyn1;
            dynamic dyn2;
            JsonArray al;

            Settings = new RestResponse()
            {
                Content = settings
            };

            jd = new JsonDeserializer();
            dyn1 = jd.Deserialize<dynamic>(Settings);

            try
            {
                StoreDatafiles = dyn1["SaveFiles"];
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            try
            {
                dyn2 = dyn1["DataFolder"];
                DataFolder = dyn2;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            // DB connections: Since data could be stored in more that one DB, it was decided to make an array of
            // SqlConnectionStringBuilder objects in the DataBases field
            try
            {
                dyn2 = dyn1["DataBases"];
                al = dyn2;
                for (int i = 0; i < al.Count; i++)
                {
                    dyn2 = al[i];
                    SqlConnectionStringBuilder scb = new()
                    {
                        DataSource = dyn2["DataSource"],
                        InitialCatalog = dyn2["InitialCatalog"],
                        UserID = dyn2["UserID"],
                        Password = dyn2["Password"]
                    };
                    ConnectionStrings.Add(scb);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

        }

    }
}
