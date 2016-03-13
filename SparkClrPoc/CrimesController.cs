using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using SparkClrPoc.Models;

namespace SparkClrPoc
{
    public class CrimesController : ApiController
    {
        private static SqlContext _sqlContext;
        private static SparkContext _sparkContext;
        private const string CrimeFilePath = @"C:\code\sparkplayground\SparkClrPoc\Data\SacramentocrimeJanuary2006.csv";

        public CrimesController()
        {
            _sparkContext = Program.SparkContext;
            var crimeDataFrame = GetSqlContext()
                .TextFile(CrimeFilePath,
                        new StructType(new List<StructField>
                        {
                            new StructField("cdatetime", new StringType()),
                            new StructField("address", new StringType()),
                            new StructField("district", new StringType()),
                            new StructField("beat", new StringType()),
                            new StructField("grid", new StringType()),
                            new StructField("crimedescr", new StringType()),
                            new StructField("ucr_ncic_code", new StringType()),
                            new StructField("latitude", new StringType()),
                            new StructField("longitude", new StringType()),
                        }))
                .Cache();
            crimeDataFrame.RegisterTempTable("crime");
            crimeDataFrame.Write().Mode(SaveMode.Overwrite).Parquet(@"c:\temp\test");

        }

        public IEnumerable<Crime> Get(string search)
        {
            return GetSqlContext()
                .Sql("SELECT crimedescr " +
                     "FROM crime " +
                     $"WHERE crimedescr LIKE '%{search}%' ")
                .Select("crimedescr")
                .Collect()
                .Select(r => new Crime
                {
                    Description = (string) r.Get(0),
                })
                .ToList();
        }

        private static SqlContext GetSqlContext()
        {
            return _sqlContext ?? (_sqlContext = new SqlContext(_sparkContext));
        }
    }
}
