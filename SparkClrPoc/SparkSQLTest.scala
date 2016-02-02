package tdunst.common

import java.io.File

import au.com.bytecode.opencsv.CSVParser
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem, FileUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{LongType, StructField, StringType, StructType}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{rdd, SparkContext, SparkConf}


/**
  * Created by toddmichael on 1/13/16.
  */
class SparkSQLTest {
  def QueryRDD() = {
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sparkSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveCtx = new HiveContext(sc)
    //val sqlContext = new SQLContext(sc)
    import hiveCtx.implicits._

    // Load our input data.
    val input = hiveCtx.read.json("/Users/toddmichael/Downloads/learning-spark-master/files/testweet.json")
    // Register the input schema RDD
     input.registerTempTable("tweets")
    hiveCtx.cacheTable("tweets")
    // Select tweets based on the retweetCount
    val topTweets = hiveCtx.sql("SELECT text, retweetCount FROM tweets ORDER BY retweetCount LIMIT 10")

    // Delete the previous output
    FileUtils.deleteQuietly(new File("/Users/toddmichael/frameworks/spark-1.6.0-bin-hadoop2.3/SparkSqlOutput.txt"))
    topTweets.write.save("/Users/toddmichael/frameworks/spark-1.6.0-bin-hadoop2.3/SparkSqlOutput.txt")

    println(topTweets.show(false))

    val topTweetText = topTweets.rdd.map(row => row.getString(0))
    FileUtils.deleteQuietly(new File("/Users/toddmichael/frameworks/spark-1.6.0-bin-hadoop2.3/SparkSqlTopTweets.txt"))
    topTweetText.saveAsTextFile("/Users/toddmichael/frameworks/spark-1.6.0-bin-hadoop2.3/SparkSqlTopTweets.txt")

    println(input.printSchema())
    //HiveThriftServer2.startWithContext(hiveCtx)

  }

  def parseTextFile() = {
    val crimeFile = "/Users/toddmichael/Downloads/SacramentocrimeJanuary2006.csv"
    val bigCrimeFile = "/Users/toddmichael/Downloads/Crimes_-_2001_to_present.csv"

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("sparkSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Load and cache the crime data
    val crimeData = sc.textFile(crimeFile).cache()

    // Strip off the file header
    val withoutHeader: RDD[String] = dropHeader(crimeData)

    // Parse each line, and create a rollup containing the count of each
    // unique occurrence of column 5 in the file
    withoutHeader.mapPartitions(lines => {
      val parser=new CSVParser(',')
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(5)).mkString(",")
      })
    }).countByValue().toList.sortBy(-_._2).foreach(println)

    val fields = List(StructField("cdatetime",StringType,nullable = true),
      StructField("address",StringType,nullable = true),
      StructField("district",StringType,nullable = true),
      StructField("beat",StringType,nullable = true),
      StructField("grid",StringType,nullable = true),
      StructField("crimedescr",StringType,nullable = true),
      StructField("ucr_ncic_code",StringType,nullable = true),
      StructField("latitude",StringType,nullable = true),
      StructField("longitude",StringType,nullable = true)).toArray
    val schema = new StructType(fields)

    //val crimesDataframe = sqlContext.createDataFrame(withoutHeader, schema)


  }

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  def createDfFromFile() = {
    val conf = new SparkConf().setAppName("sparkSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveCtx = new HiveContext(sc)
    val crimeFile = "/Users/toddmichael/Downloads/SacramentocrimeJanuary2006.csv"
    sqlContext.load("com.databricks.spark.csv", Map("path" -> crimeFile, "header" -> "true")).registerTempTable("crimes2")

    sqlContext.sql(
      """
        select `crimedescr` as primaryType, COUNT(*) AS times
        from crimes
        group by `crimedescr`
        order by times DESC
      """).save("/tmp/agg.csv", "com.databricks.spark.csv")

    merge("/tmp/agg.csv", "agg.csv")

  }

  private def createFile(df: DataFrame, file: String, header: String): Unit = {
    FileUtil.fullyDelete(new File(file))
    val tmpFile = "tmp/" + System.currentTimeMillis() + "-" + file
    df.distinct.save(tmpFile, "com.databricks.spark.csv")
  }

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
  }

  def testDatabricksCSVParse() = {
    val conf = new SparkConf().setAppName("sparkSqlTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val crimeFile = "/Users/toddmichael/Downloads/SacramentocrimeJanuary2006.csv"
    val df = sqlContext.read.format("com.databricks.spark.csv").options(Map("header"->"true", "inferSchema"->"true")).load(crimeFile)

    df.printSchema()
    df.registerTempTable("crimes")

    val crimeCounts = sqlContext.sql(
      """
        select `crimedescr` as primaryType, COUNT(*) AS times
        from crimes
        group by `crimedescr`
        order by times DESC
      """)

    println(crimeCounts.collectAsList())

    // Save the table in Hive
    // TODO: Work on drop-if-exists
    // df.write.saveAsTable("crimes")

    // write the entire dataframe our to disk (uses the Hadoop API)
    val crimeData = "/tmp/crimedata.csv"
    df.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite)
      .options(Map("header"->"true")).save(crimeData)

    // Write the aggregate totals out to disk (uses the Hadoop API)
    val crimeAgg = "/tmp/crimeAgg.csv"
    crimeCounts.write.format("com.databricks.spark.csv").mode(SaveMode.Overwrite)
      .options(Map("header"->"false")).save(crimeAgg)

    // Merge the Hadoop FS output and write to local file system
    merge(crimeAgg, "crimeCounts.csv")

  }

}
