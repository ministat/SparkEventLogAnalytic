package cn.net.local

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkEventLogAnalytic {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: <history_log_file> <analytic_out_dir>")
      System.exit(1)
    }
    val jsonFile = args(0)
    var outDir = args(1)
    val outputFilename = FilenameUtils.getBaseName(jsonFile)
    val spark = SparkSession.builder().appName("TestEventLog").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.json(jsonFile)
    val df2 = df.filter("Event='SparkListenerStageCompleted'").select("`Stage Info`.*")
    df2.createOrReplaceTempView("t2")
    val df4 = spark.sql("select 'Submission Time','Completion Time', 'Number of Tasks', 'Stage ID', t3.col.* from t2 lateral view explode(Accumulables) t3")
    df4.createOrReplaceTempView("t4")
    val result = spark.sql("select Name, sum(Value) as value from t4 group by Name order by Name") //.show(100,false)
    result.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(s"${outDir}/${outputFilename}")
    result.show(false)
    spark.close()
  }
}
