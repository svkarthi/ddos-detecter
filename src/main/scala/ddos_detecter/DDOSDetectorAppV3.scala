package ddos_detecter

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import java.time.format.DateTimeFormatter
import scala.util._

object DDOSDetectorAppV3 extends App{

  val conf  =  new SparkConf().setAppName("DDOSDetectorAppV3").set("log4j.rootCategory","WARN, console")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder.config(conf).getOrCreate()

  val fieldList =  List("ip","f1","f2")
  import spark.implicits._
  def toTimeStamp(s:String):Option[java.sql.Timestamp] ={
    //java.sql.Timestamp.valueOf(java.time.LocalDateTime.now())
    //25/May/2015:23:11:15
   Try( java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(s.substring(1).replace(" ",""),
      DateTimeFormatter.ofPattern("dd/MMM/yyyy:kk:mm:ss")))) match {
     case Success(s) => Some(s)
     case Failure(f) => None
   }
  }
  case class BotNetType( ip:String, dateTime:Option[java.sql.Timestamp], httpMethod:String,browser:String)
  def dfShow(df:DataFrame, batchId:Long) ={
    val ts = df.map( x => {
      val value = x.getAs[String]("value").split(" ")
      BotNetType(value(0),
        toTimeStamp(value(3)),
        value(5),
        value(9))
    })
    ts.write.csv("C:\\Kafka\\kafka_tgt3")
  }




  val apacheLogDF = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "incomingips")
    .option("startingOffsets","earliest")
  .option("failOnDataLoss","false")
    .load().select('value.cast("String"))

  val df  = apacheLogDF.map( x => {
    val value =  x.getAs[String]("value").split(" ")
    BotNetType(value(0),
      toTimeStamp(value(3)),
      value(5),
      value(9)
    )
  })

  def writeBatchCSV(df:DataFrame, batchId:Long) ={

    df.write.mode(SaveMode.Append).csv("C:\\Kafka\\vulnerableIPs")
  }

  val windowed = df//.withWatermark("dateTime","1 minute")
    .groupBy('ip,window($"dateTime","1 minute")).count().filter('count>=25)

  windowed.select('ip,'count,$"window.start", $"window.end")//.writeStream.foreachBatch(writeBatchCSV _).start().awaitTermination()
    .writeStream
    .format("console")
    .outputMode("complete")
    .option("checkpointLocation", "chk-point-dir")
    .trigger(Trigger.ProcessingTime("1 minute"))
    .start().awaitTermination()

  /*
windowed.select('ip,'count)//.writeStream.foreachBatch(writeBatchCSV _).start().awaitTermination()
  .writeStream
  .format("csv")
  .outputMode("complete")
  .option("path", "C:\\Kafka\\vulnerableIP1s")
  .option("checkpointLocation", "chk-point-dir1")
  .option("failOnDataLoss","false")
  .trigger(Trigger.ProcessingTime("1 minute"))
  .start().awaitTermination()
*/
//windowed.select('ip,'count).writeStream.outputMode("complete").format("console").start().awaitTermination()


}
