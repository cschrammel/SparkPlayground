using System;
using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;
using SparkClrPoc.Models;

namespace SparkClrPoc
{
    public class FixedWidthController : ApiController
    {
        private static SqlContext _sqlContext;
        private static SparkContext _sparkContext;
        private const string CrimeFilePath = @"C:\code\sparkplayground\SparkClrPoc\Data\foo.txt";

        public FixedWidthController()
        {
         
        }

        public IEnumerable<string> Get()
        {
            _sparkContext = Program.SparkContext;
            var crimeDataFrame = GetSqlContext()
                .TextFile(CrimeFilePath)
                .Map(l=>((string)l.Get(0)).Substring(0,1))
                .Cache();

            var tempTdd = _sparkContext.TextFile(CrimeFilePath).Map(l => new object[] { l.Substring(0, 1) });

            var data = GetSqlContext().CreateDataFrame(tempTdd, new StructType(new List<StructField>
                        {
                            new StructField("Line", new StringType())
                        }));

            data.Show();
            return data.Collect().Select(l=>l.ToString()).ToList();

            return new List<string>();
        }

        private static SqlContext GetSqlContext()
        {
            return _sqlContext ?? (_sqlContext = new SqlContext(_sparkContext));
        }
    }
}
