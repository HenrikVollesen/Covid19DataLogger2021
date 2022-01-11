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
    class MainClass
    {
        // Optional to store data files on disk
        private static bool StoreDatafiles = false;

        // Base folder for storage of coronavirus data - choose your own folder IF you want to save the data files
        private static string DataFolder = @"D:\Data\coronavirus\stats\CountryStats\";

        // If command line contains '-settingsfile', the next argument should be path to a settingsfile.
        private static string SettingsPath = "Settings.json";

        // Start properties if no command line arguments
        // !!!Remember to change the DataSource name to your local MSSQL!!!
        private static string DataSourceFile = @"SomeLocalSQLServer\\SomeSQLInstance";
        private static string InitialCatalogFile = "Covid-19_Stats";
        private static string UserIDFile = "psu_Covid19_Reader";
        private static string PasswordFile = "Corona_2020";

        private static List<SqlConnectionStringBuilder> ConnectionStrings = new List<SqlConnectionStringBuilder>();

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

            bool SaveFiles = StoreDatafiles;
            string path = DataFolder;

            // data may be logged to more than DB, therefore the foreach loop
            foreach (SqlConnectionStringBuilder s in ConnectionStrings)
            {
                LoggerSettings loggerSettings = new LoggerSettings()
                {
                    SaveFiles = SaveFiles, 
                    DataFolder = path,
                    ConnString = s.ConnectionString
                };

                Covid19_DataLogger theLogger = new Covid19_DataLogger(loggerSettings);
                theLogger.ScrapeCovid19Data();

                SaveFiles = false; // Only save the first time
                path = "";
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
                    DataFolder = dyn2;

                    //DataFolder = datafolder + @"CountryStats\";
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
