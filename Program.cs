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
        private static bool StoreDatafiles = false;

        // Base folder for storage of coronavirus data - choose your own folder IF you want to save the data files
        private static string DataFolder = @"D:\Data\coronavirus\stats\CountryStats\";

        // If command line contains '-settingsfile', the next argument should be path to a settingsfile.
        private static string SettingsPath = "Settings.json";

        // resource URL for REST API
        private const string ClientString1 = "https://disease.sh/v3/covid-19/historical/";
        private const string ClientString2 = "?lastdays=";

        // Start properties if no command line arguments
        private const string Filename_Stats = "LatestStats_";
        // !!!Remember to change the Server name to your local MSSQL!!!
        private static string DataSourceFile = @"SomeLocalSQLServer\\SomeSQLInstance";
        private static string InitialCatalogFile = "Covid-19_Stats";
        private static string UserIDFile = "Covid19_Reader";
        private static string PasswordFile = "Corona_2020";

        private static List<SqlConnectionStringBuilder> ConnectionStrings = new List<SqlConnectionStringBuilder>();

        // SQL command for getting predefined countries (there should be 185 countries)
        private static string GetCountriesCommand = "SELECT Alpha_2_code FROM GetAPICountries()";

        private static RestRequest request = null;
        private static IRestResponse response_Stats = null;

        private static readonly int Delay = 200;

        // Just for info: This is the woman who started it all...
        // https://engineering.jhu.edu/case/faculty/lauren-gardner/
        private static DateTime DayZero = new DateTime(2020, 1, 22);


        static void Main(string[] args)
        {
            // Start by looking for settings in command line args
            ParseCommandline(args);

            if (ConnectionStrings.Count == 0)
            {
                SqlConnectionStringBuilder scb = new SqlConnectionStringBuilder()
                {
                    DataSource = DataSourceFile,
                    InitialCatalog = InitialCatalogFile,
                    UserID = UserIDFile,
                    Password = PasswordFile
                };

                ConnectionStrings.Add(scb);
            }

            if (StoreDatafiles)
            {
                if (!Directory.Exists(DataFolder))
                {
                    Directory.CreateDirectory(DataFolder);
                }
            }

            Get_Data();
        }

        static private void Get_Data()
        {
            List<string> IsoCodeList = new List<string>();

            // In this first block of using the DB connection, we will read the country IsoCodes of the
            // (currently) 185 countries. These IsoCodes are a unique identifier for a country.
            // Example: 'DK' is Denmark
            SqlConnection conn = new SqlConnection(ConnectionStrings[0].ConnectionString);
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

            InsertDates();

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

            // Start logging data for each country...
            Console.WriteLine("Covid19DataLogger2021 logging...\n");
            foreach (string isoCode in IsoCodeList)
            {
                conn.Open();

                using SqlCommand getLastBadFunc = new("SELECT dbo.FirstBadDate(N'" + isoCode + "')", conn);
                //getLastBadFunc.CommandType = CommandType.StoredProcedure;
                //getLastBadFunc.Parameters.AddWithValue("@Param1", isoCode);
                //DateTime rowsAffected = (DateTime)getLastBadFunc.ExecuteScalar();

                //object o = getLastBadFunc.ExecuteScalar();

                //string LastBadDateString = "SELECT TOP 1 date FROM DimDate " +
                //    "WHERE(date NOT IN " +
                //        "(SELECT date FROM FactCovid19Stat WHERE(Alpha_2_code = '" + isoCode + "'))) " +
                //    "ORDER BY date ASC";

                //SqlCommand getLastBad = new(LastBadDateString, conn);
                try
                {
                    object o = getLastBadFunc.ExecuteScalar();
                    //object v = getLastBad.ExecuteScalar();
                    if (o is DateTime) // IF days are missing, we request data from the first missing date
                    {
                        DateTime LastBadDate = (DateTime)o;
                        DaysSinceZero = now - LastBadDate;
                        DaysBack = DaysSinceZero.Days;

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

                        //DateTime theday = DayZero;
                        DateTime theday = LastBadDate;
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
                    }
                    else
                    {
                        Console.WriteLine(isoCode + " OK");
                    }
                }
                catch (Exception ex)
                {
                    string test = ex.Message;
                }

                conn.Close();

                // Sometimes the REST Api server will bitch if you try too aggresively to download data - give it a 'rest' tee-hee
                //Thread.Sleep(Delay);
            }
        }

        private static bool InsertDates()
        {
            bool result = false;
            SqlConnection conn = new SqlConnection(ConnectionStrings[0].ConnectionString);
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

            DateTime now = DateTime.Now;
            TimeSpan DaysSinceLast = now - FirstMissingDate;
            int DaysBack = DaysSinceLast.Days;
            if (DaysBack > 0) // We will insert days up to yesterday. Probably no data for today anyway
            {
                DateTime theday = FirstMissingDate;
                // Step precisely 1 day forward

                // This loop will run from FirstMissingDate to yesterday, incrementing theDay by 24 hours
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

                    using SqlCommand cmd2 = new("Save_Date", conn);
                    cmd2.CommandType = CommandType.StoredProcedure;
                    cmd2.Parameters.AddWithValue("@date", SaveDate);
                    int rowsAffected = cmd2.ExecuteNonQuery();


                    theday += nextday;
                }
                result = true;
            }

            conn.Close();
            return result;
        }

        static private void SaveStatData(string SaveDate, string isoCode, long day_cases, long day_deaths, long day_recovered, SqlConnection conn)
        {
            if (true) // for now
            {
                using SqlCommand cmd2 = new("Save_Date", conn);
                cmd2.CommandType = CommandType.StoredProcedure;
                cmd2.Parameters.AddWithValue("@date", SaveDate);
                int rowsAffected = cmd2.ExecuteNonQuery();
            }

            using (SqlCommand cmd2 = new("Save_DayStat", conn))
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

        static private void ParseCommandline(string[] args)
        {
            if (args.Length > 0)
            {
                string arg0 = args[0].ToLower().Trim();
                if (arg0 == "-settingsfile")
                {
                    if (args.Length > 1)
                    {
                        SettingsPath = args[1];
                    }
                }
            }
            if (File.Exists(SettingsPath))
            {
                IRestResponse Settings;
                JsonDeserializer jd;
                dynamic dyn1;
                dynamic dyn2;
                JsonArray al;

                Settings = new RestResponse()
                {
                    Content = File.ReadAllText(SettingsPath)
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
                    string datafolder = dyn2;

                    DataFolder = datafolder + @"CountryStats\";
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
                        DataSourceFile = dyn2["DataSource"];
                        InitialCatalogFile = dyn2["InitialCatalog"];
                        UserIDFile = dyn2["UserID"];
                        PasswordFile = dyn2["Password"];
                        SqlConnectionStringBuilder scb = new SqlConnectionStringBuilder()
                        {
                            DataSource = DataSourceFile,
                            InitialCatalog = InitialCatalogFile,
                            UserID = UserIDFile,
                            Password = PasswordFile
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
}
