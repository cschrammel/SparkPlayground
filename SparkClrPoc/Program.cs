using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

namespace SparkClrPoc
{
    class Program
    {
        private static SqlContext _sqlContext;
        internal static SparkContext SparkContext;

        static void Main(string[] args)
        {
            SparkContext = CreateSparkContext();
            SparkContext.SetCheckpointDir(Path.GetTempPath());

            var schema = new StructType(new List<StructField>
            {
                new StructField("cdatetime", new StringType(), true),
                new StructField("address", new StringType(), true),
                new StructField("district", new StringType(), true),
                new StructField("beat", new StringType(), true),
                new StructField("grid", new StringType(), true),
                new StructField("crimedescr", new StringType(), true),
                new StructField("ucr_ncic_code", new StringType(), true),
                new StructField("latitude", new StringType(), true),
                new StructField("longitude", new StringType(), true),
            });

            var crimeDataFrame = GetSqlContext()
                .TextFile(@"C:\github\sparkplayground\SparkClrPoc\Data\SacramentocrimeJanuary2006.csv",
                    schema);

            //var row = crimeDataFrame.First();
            //var schema = row.GetSchema();

            crimeDataFrame.RegisterTempTable("crime");

            var sqlResults = GetSqlContext().Sql("SELECT crimedescr, COUNT(*) AS Count " +
                                                 "FROM crime " +
                                                 "WHERE crimedescr LIKE '%THEFT%' " +
                                                 "GROUP BY crimedescr " +
                                                 "ORDER BY 2 DESC ");
            sqlResults
                .Select("crimedescr", "Count")
                .Collect()
                .ToList()
                .ForEach(Console.WriteLine);
            
            //SparkContext.Stop();
        }

        private static SparkContext CreateSparkContext()
        {
            //var conf = new SparkConf();
            //conf.Set("spark.local.dir", @"C:\temp");
            return new SparkContext("local", "sparkSqlTest");
        }

        private static SqlContext GetSqlContext()
        {
            return _sqlContext ?? (_sqlContext = new SqlContext(SparkContext));
        }
    }
}
