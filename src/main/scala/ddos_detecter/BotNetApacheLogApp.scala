package ddos_detecter


import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import scala.util._

object BotNetApacheLogApp extends App{
  println("jai")

  val conf  =  new SparkConf().setAppName("hello").set("log4j.rootCategory","WARN, console")
  conf.setMaster("local[*]")

  val spark = SparkSession.builder.config(conf).getOrCreate()
  import spark.implicits._

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

  case class BotNetType( ip:String, dateTime:Option[java.sql.Timestamp], httpMethod:String,browser:String)
  val botNetLogDs =  spark.read.option("delimiter"," ").csv("C:\\phData\\apache-access-log.txt").map(
     x => {
       BotNetType(x.getAs[String](0),
         toTimeStamp(x.getAs[String](3)),
         x.getAs[String](5),
         x.getAs[String](9)
       )
     }
  )
  botNetLogDs.select('ip,date_trunc("Minute",'dateTime).as('date)).
    groupBy('ip,'date).agg(count("*").as("cnt")).where('cnt>20)
    //.select(max('cnt))
        .show(false)

  val df = Seq(("mykey","myvalue")).toDF("key","value")
  val ds = botNetLogDs
    //.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .select(lit("ip").as("key"),$"ip".as("value"))
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("topic", "apachelog")
    .save()

}
