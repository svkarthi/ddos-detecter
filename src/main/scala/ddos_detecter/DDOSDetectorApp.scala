package ddos_detecter


import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import scala.util._

object DDOSDetectorApp extends App{

  val conf  =  new SparkConf().setAppName("DDOSDetectorApp").set("log4j.rootCategory","WARN, console")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  val fieldList =  List("ip","f1","f2")

  def toTimeStamp(s:String):Option[java.sql.Timestamp] ={
    //java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())
    //25/May/2015:23:11:15
   Try( java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(s.substring(1).replace(" ",""),
      DateTimeFormatter.ofPattern("dd/MMM/yyyy:kk:mm:ss")))) match {
     case Success(s) => Some(s)
     case Failure(f) => None
   }
  }
  import spark.implicits._

  case class BotNetType( ip:String, dateTime:Option[java.sql.Timestamp], httpMethod:String,browser:String)

  val vulnerableIPDFs = spark
    .read
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "apachelog")
    .option("startingOffsets","earliest")
    .load().select('value.cast("String")).distinct()
//  df.printSchema()


  val incomingIpsDF = spark.readStream.format("kafka")
    .option("checkpointLocation", "C:\\Kafka\\checkpointloc1\\")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "incomingips")
    .option("startingOffsets","latest")

    .load()

  incomingIpsDF.select('value.cast("String")).writeStream.foreachBatch(batchFunction _).start().awaitTermination()

  def batchFunction(df:DataFrame, batchId:Long) ={
    df.printSchema()
    df.selectExpr("CAST(value AS STRING)").as('in).join(vulnerableIPDFs.as('vu),$"in.value"===
      $"vu.value","inner").select($"in.value").write.mode(SaveMode.Append).csv("C:\\Kafka\\kafka_tgt1")
  }
}
