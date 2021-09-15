using System;
using RestSharp;
using RestSharp.Serialization.Json;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Data;
using System.Collections.Generic;

namespace Covid19DataLogger2021
{
    class Program_Covid19DataLogger
    {
        // Optional to store data files on disk
        private static bool StoreDatafiles = true;
        // Base folder for storage of coronavirus data - choose your own folder IF you want to save the data files
        private static string DataFolder = @"D:\Data\coronavirus\stats\CountryStats\";

        private static string Filename_Stats = "LatestStats_";
        private static string ClientString1 = "https://disease.sh/v3/covid-19/historical/";
        private static string ClientString2 = "?lastdays=";

        // !!!Remember to change the Server name to your local MSSQL!!!
        private static string ConnectionString = "Server=SERVER2019\\SQLSERVER2019DEV;Database=Covid19DB;Trusted_Connection=True";

        private static string Command = "SELECT Alpha_2_code FROM GetAPICountries()";

        private static RestRequest request = null;
        private static IRestResponse response_Stats = null;

        private static readonly int Delay = 300;

        static void Main(string[] args)
        {
            if (!Directory.Exists(DataFolder))
            {
                Directory.CreateDirectory(DataFolder);
            }

            Get_Data();
        }

        static private void Get_Data()
        {
            // Just for info: This is the woman who started it all...
            // https://engineering.jhu.edu/case/faculty/lauren-gardner/
            DateTime DayZero = new DateTime(2020, 1, 22);
            DateTime now = DateTime.Now;
            TimeSpan DaysSinceZero = now - DayZero;

            // We will log all the way from Day Zero
            //    (for now - this can be improved. But at least the DB will not insert data that are already there)
            int DaysBack = DaysSinceZero.Days;

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

            List<string> IsoCodeList = new List<string>();

            SqlConnection conn = new SqlConnection(ConnectionString);
            conn.Open();

            using (SqlCommand cmd = new SqlCommand(Command, conn))
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

            // Start logging...
            Console.WriteLine("Covid19DataLogger2021 logging...\n");
            foreach (string isoCode in IsoCodeList)
            {
                conn.Open();

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

                DateTime theday = DayZero;
                // Step precisely 1 day forward
                TimeSpan nextday = new TimeSpan(1, 0, 0, 0);

                // This loop will run from DayZero to yesterday, incrementing theDay by 24 hours
                for (int i = DaysBack; i > 0; i--)
                {
                    // A string must be made with the date in US date format:
                    // SaveDate = "3/4/21"
                    // This is because the data are stored in the JSON response as a key–value pair where the key is this date.
                    string d = theday.Day.ToString();
                    string m = theday.Month.ToString();
                    string y = theday.Year.ToString();
                    y = y.Substring(2, 2);
                    string SaveDate = m + "/" + d + "/" + y;
                    try
                    {
                        day_cases = cases[SaveDate];
                        day_deaths = deaths[SaveDate];
                        day_recovered = recovered[SaveDate];

                        SaveStatData(SaveDate, isoCode, day_cases, day_deaths, day_recovered, conn);
                    }
                    catch (Exception e)
                    {
                        string test = e.Message;
                    }
                    theday += nextday;
                }

                conn.Close();

                // Sometimes the REST Api server will bitch if you try too aggresively to download data - give it a 'rest' tee-hee
                Thread.Sleep(Delay);
            }
        }

        static private void SaveStatData(string SaveDate, string isoCode, long day_cases, long day_deaths, long day_recovered, SqlConnection conn)
        {
            if (true) // for now
            {
                using SqlCommand cmd2 = new SqlCommand("Save_Date", conn);
                cmd2.CommandType = CommandType.StoredProcedure;
                cmd2.Parameters.AddWithValue("@date", SaveDate);
                int rowsAffected = cmd2.ExecuteNonQuery();
            }

            using (SqlCommand cmd2 = new SqlCommand("Save_DayStat", conn))
            {
                cmd2.CommandType = CommandType.StoredProcedure;
                cmd2.Parameters.AddWithValue("@Alpha_2_code", isoCode);
                cmd2.Parameters.AddWithValue("@date", SaveDate);
                cmd2.Parameters.AddWithValue("@confirmed", day_cases);
                cmd2.Parameters.AddWithValue("@deaths", day_deaths);
                cmd2.Parameters.AddWithValue("@recovered", day_recovered);
                int rowsAffected = cmd2.ExecuteNonQuery();
            }
        }
    }

}
