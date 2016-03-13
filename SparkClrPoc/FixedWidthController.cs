using System.Collections.Generic;
using System.Linq;
using System.Web.Http;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Sql;

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

        public IEnumerable<Model> Get()
        {
            _sparkContext = Program.SparkContext;
            var crimeDataFrame = GetSqlContext()
                .TextFile(CrimeFilePath)
                .Cache();

            var tempTdd = _sparkContext.TextFile(CrimeFilePath)
                .Map(l => new object[]
                        {
                            int.Parse(l.Substring(0, 3)),
                            int.Parse(l.Substring(4, 3)),
                            int.Parse(l.Substring(8, 4)),
                        });

            var data = GetSqlContext().CreateDataFrame(tempTdd, new StructType(new List<StructField>
                        {
                            new StructField("Field1", new IntegerType()),
                            new StructField("Field2", new IntegerType()),
                            new StructField("Field3", new IntegerType())
                        }));

            data.Show();
            data.RegisterTempTable("data");
            
            return GetSqlContext().Sql("SELECT Field1, Field2, Field3 FROM data")
                .Collect()
                .Select(l => new Model
                {
                    Field1 = l.Get("Field1"),
                    Field2 = l.Get("Field2"),
                    Field3 = l.Get("Field3"),
                }).ToList();
        }

        private static SqlContext GetSqlContext()
        {
            return _sqlContext ?? (_sqlContext = new SqlContext(_sparkContext));
        }
    }

    public class Model
    {
        public int Field1 { get; set; }
        public int Field2 { get; set; }
        public int Field3 { get; set; }
    }
}
