package cn.net.local

import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}

import scala.collection.JavaConverters._
class ArgumentsParser{
  @Option(name="-i", aliases=Array("--input-json-file"), usage="Specify the json file")
  var inputJsonFile: String = null

  @Option(name="-o", aliases=Array("--output-dir"), usage="Specify the output directory")
  var outputDir: String = null

  @Option(name="-s", aliases=Array("--stage"), usage="Generate stage statistics, default is true")
  var stageStat: Boolean = true

  @Option(name="-t", aliases=Array("--task"), usage="Generate task statistics, default is false")
  var taskStat: Boolean = false

  def doParse(arguments: Array[String]): Unit = {
    val parser = new CmdLineParser(this)
    if (arguments.length < 1) {
      parser.printUsage(System.out)
      System.exit(-1)
    }
    try {
      parser.parseArgument(arguments.toList.asJava)
    }
    catch {
      case clEx: CmdLineException =>
        System.out.println("ERROR: Unable to parse command-line options: " + clEx)
    }
  }
}

object SparkEventLogAnalytic {
  var argumentsParser: ArgumentsParser = null

  private def stageInfo(spark: SparkSession): Unit = {
    val jsonFile = argumentsParser.inputJsonFile
    var outDir = argumentsParser.outputDir
    val outputFilename = "stage" + FilenameUtils.getBaseName(jsonFile)

    val df = spark.read.json(jsonFile)
    val df2 = df.filter("Event='SparkListenerStageCompleted'").select("`Stage Info`.*")
    df2.createOrReplaceTempView("t2")
    val df4 = spark.sql("select 'Submission Time','Completion Time', 'Number of Tasks', 'Stage ID', t3.col.* from t2 lateral view explode(Accumulables) t3")
    df4.createOrReplaceTempView("t4")
    val result = spark.sql("select Name, sum(Value) as value from t4 group by Name order by Name") //.show(100,false)
    result.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(s"${outDir}/${outputFilename}")
    result.show(false)
  }

  private def taskInfo(spark: SparkSession): Unit = {
    val jsonFile = argumentsParser.inputJsonFile
    var outDir = argumentsParser.outputDir
    val outputFilename = "task" + FilenameUtils.getBaseName(jsonFile)

    val df = spark.read.json(jsonFile)
    val df2 = df.filter("Event='SparkListenerTaskEnd'").select("Stage ID", "Task Info.*", "Task Metrics.*")
    val result = df2.select("Input Metrics.*","Executor CPU Time","Finish Time","Locality")
    result.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(s"${outDir}/${outputFilename}")
    result.show(false)
  }

  def main(args: Array[String]): Unit = {
    argumentsParser = new ArgumentsParser()
    argumentsParser.doParse(args)
    if (!argumentsParser.stageStat && !argumentsParser.taskStat) {
      return
    }
    val spark = SparkSession
      .builder()
      .appName("Spark event log analytics")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if (argumentsParser.stageStat) {
      stageInfo(spark)
    }
    if (argumentsParser.taskStat) {
      taskInfo(spark)
    }
    spark.close()
  }
}
