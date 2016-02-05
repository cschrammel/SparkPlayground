using System;
using System.IO;
using Microsoft.Owin.Hosting;
using Microsoft.Spark.CSharp.Core;

namespace SparkClrPoc
{
    public class Program
    {
        public static SparkContext SparkContext;

        static void Main(string[] args)
        {
            string baseAddress = "http://localhost:9000/";

            SparkContext = CreateSparkContext();
            SparkContext.SetCheckpointDir(Path.GetTempPath());

            using (WebApp.Start<Startup>(baseAddress))
            {
                Console.ReadLine();
            }

            SparkContext.Stop();
        }

        private static SparkContext CreateSparkContext()
        {
            return new SparkContext("local", "sparkSqlTest");
        }

    }
}
