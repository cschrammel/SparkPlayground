using System;
using Microsoft.Owin.Hosting;
using Microsoft.Spark.CSharp.Core;

namespace SparkClrPoc
{
    public class Program
    {
        public static SparkContext SparkContext;

        static void Main(string[] args)
        {
            SparkContext = CreateSparkContext();

            using (WebApp.Start<Startup>("http://localhost:9000/"))
            {
                Console.ReadLine();
            }

            SparkContext.Stop();
        }

        private static SparkContext CreateSparkContext()
        {
            var conf = new SparkConf();
            conf.Set("spark.local.dir", @"C:\temp");
            return new SparkContext(conf);
        }
    }
}
