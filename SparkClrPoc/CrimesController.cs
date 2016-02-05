using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

namespace SparkClrPoc
{
    public class CrimesController : ApiController
    {
        private static SqlContext _sqlContext;
        private static SparkContext _sparkContext;

        public CrimesController()
        {
            _sparkContext = Program.SparkContext;

            var crimeDataFrame = GetSqlContext()
                .TextFile(@"C:\github\sparkplayground\SparkClrPoc\Data\SacramentocrimeJanuary2006.csv",
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
        }

        public IEnumerable<string> Get(string search)
        {
            return GetSqlContext().Sql("SELECT crimedescr, COUNT(*) AS Count " +
                                       "FROM crime " +
                                       $"WHERE crimedescr LIKE '%{search.ToUpper()}%' " +
                                       "GROUP BY crimedescr " +
                                       "ORDER BY 2 DESC ")
                .Select("crimedescr", "Count")
                .Collect()
                .OrderByDescending(r => r.Get(1))
                .Select(r => (string) r.Get(0) + "(" + (int) r.Get(1) + ")")
                .ToList();
        }

        private static SqlContext GetSqlContext()
        {
            return _sqlContext ?? (_sqlContext = new SqlContext(_sparkContext));
        }
    }
}
