package ddos_detecter
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import scala.util._

object BotNetApacheLogIngestApp extends App{

  val conf  =  new SparkConf().setAppName("BotNetApacheLogIngestApp").set("log4j.rootCategory","WARN, console")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  val botNetLogDs =  spark.read.textFile("C:\\phData\\apache-access-log.txt")

  //botNetLogDs.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").show()
  botNetLogDs.selectExpr("CAST(value AS STRING)").write
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("topic", "incomingips")
    .save()
}
