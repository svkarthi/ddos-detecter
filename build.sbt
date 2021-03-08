name := "ddos-detecter"

version := "1.0"

scalaVersion := "2.11.11"

javaOptions in run ++= Seq(
  "-Dlog4j.off=true",
  "-Dlog4j.configuration=./config/log4j.properties")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.7"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.4.7"