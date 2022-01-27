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

        static void Main(string[] args)
        {
            // Pass cmd line string directly to ctor of Covid19_DataLogger - for now.
            // In next iteration, parse it with a new ParseCommandline() that extracts JSON to a new ctor
            string SettingsPath;

            if (args.Length > 0)
            {
                string arg0 = args[0].ToLower().Trim();
                if (arg0 == "-settingsfile")
                {
                    if (args.Length > 1)
                    {
                        SettingsPath = args[1];
                        if (File.Exists(SettingsPath))
                        {

                        }

                    }
                }
            }

            Covid19_DataLogger theLogger = new(args);
            theLogger.Log();
        }

        //static private void ParseCommandline(string[] args)
        //{
        //    if (args.Length > 0)
        //    {
        //        string arg0 = args[0].ToLower().Trim();
        //        if (arg0 == "-settingsfile")
        //        {
        //            if (args.Length > 1)
        //            {
        //                SettingsPath = args[1];
        //            }
        //        }
        //    }
        //    if (File.Exists(SettingsPath))
        //    {
        //        IRestResponse Settings;
        //        JsonDeserializer jd;
        //        dynamic dyn1;
        //        dynamic dyn2;
        //        JsonArray al;

        //        Settings = new RestResponse()
        //        {
        //            Content = File.ReadAllText(SettingsPath)
        //        };

        //        jd = new JsonDeserializer();
        //        dyn1 = jd.Deserialize<dynamic>(Settings);

        //        try
        //        {
        //            StoreDatafiles = dyn1["SaveFiles"];
        //        }
        //        catch (Exception e)
        //        {
        //            Console.WriteLine(e.Message);
        //        }

        //        try
        //        {
        //            dyn2 = dyn1["DataFolder"];
        //            DataFolder = dyn2;

        //            //DataFolder = datafolder + @"CountryStats\";
        //        }
        //        catch (Exception e)
        //        {
        //            Console.WriteLine(e.Message);
        //        }

        //        // DB connections: Since data could be stored in more that one DB, it was decided to make an array of
        //        // SqlConnectionStringBuilder objects in the DataBases field
        //        try
        //        {
        //            dyn2 = dyn1["DataBases"];
        //            al = dyn2;
        //            for (int i = 0; i < al.Count; i++)
        //            {
        //                dyn2 = al[i];
        //                DataSourceFile = dyn2["DataSource"];
        //                InitialCatalogFile = dyn2["InitialCatalog"];
        //                UserIDFile = dyn2["UserID"];
        //                PasswordFile = dyn2["Password"];
        //                SqlConnectionStringBuilder scb = new SqlConnectionStringBuilder()
        //                {
        //                    DataSource = DataSourceFile,
        //                    InitialCatalog = InitialCatalogFile,
        //                    UserID = UserIDFile,
        //                    Password = PasswordFile
        //                };
        //                ConnectionStrings.Add(scb);
        //            }
        //        }
        //        catch (Exception e)
        //        {
        //            Console.WriteLine(e.Message);
        //        }
        //    }

        //}

    }
}
