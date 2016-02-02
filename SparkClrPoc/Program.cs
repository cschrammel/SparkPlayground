using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.ObjectModel;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

namespace SparkClrPoc
{
    class Program
    {
        private static SqlContext sqlContext;
        internal static SparkContext SparkContext;

        static void Main(string[] args)
        {
            SparkContext = CreateSparkContext();
            SparkContext.SetCheckpointDir(Path.GetTempPath());

            var crimeDataFrame = GetSqlContext().Read().Json(@"data\SacramentocrimeJanuary2006.csv");

            SparkContext.Stop();
        }

        // Creates and returns a context
        private static SparkContext CreateSparkContext()
        {
            var conf = new SparkConf();
            return new SparkContext(conf);
        }

        private static SqlContext GetSqlContext()
        {
            return sqlContext ?? (sqlContext = new SqlContext(SparkContext));
        }
    }
}
