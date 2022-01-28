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
                            Covid19_DataLogger theLogger = new();
                            theLogger.Log(File.ReadAllText(SettingsPath));
                        }
                    }
                }
            }
        }


    }
}

