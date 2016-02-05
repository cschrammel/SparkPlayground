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
        
        public CrimesController()
        {
            SparkContext = Program.SparkContext;
        }

        public IEnumerable<string> Get(string search)
        {
            return CrimeQuery(search).Select(Convert.ToString).ToList();
        }

        private static List<string> CrimeQuery(string search)
        {
            var schema = new StructType(new List<StructField>
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
            });

            var crimeDataFrame = GetSqlContext()
                .TextFile(@"C:\github\sparkplayground\SparkClrPoc\Data\SacramentocrimeJanuary2006.csv",
                    schema);

            crimeDataFrame.RegisterTempTable("crime");

            var sqlQuery = "SELECT crimedescr, COUNT(*) AS Count " +
                           "FROM crime " +
                           $"WHERE crimedescr LIKE '%{search}%' " +
                           "GROUP BY crimedescr " +
                           "ORDER BY 2 DESC ";
            var sqlResults = GetSqlContext().Sql(sqlQuery);
            var results = sqlResults
                .Select("crimedescr", "Count")
                .Collect()
                .ToList()
                .OrderByDescending(r => r.Get(1))
                .Select(r => (string) r.Get(0) + "(" + (int) r.Get(1) + ")")
                .ToList();

            return results;
        }


        private static SqlContext GetSqlContext()
        {
            return _sqlContext ?? (_sqlContext = new SqlContext(SparkContext));
        }

        private static SparkContext SparkContext { get; set; }
    }
}
