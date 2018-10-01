import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.sql.Timestamp

import org.apache.spark.sql.streaming.OutputMode

import scala.io.Source


object StructuredWatermarkingWC extends App {
  """
    | Structured Streaming Example:
    | Word Count with Watermarking
    |
    | run nc -lk 9000 in Terminal, start app and type some words in Terminal
  """.stripMargin

  // Setup Spark
  val spark = SparkSession
    .builder
    .appName("StructuredWatermarkingWC")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  // Words dictionary
  val wordStream = getClass.getResourceAsStream("/words.txt")
  val wordDict = Source.fromInputStream(wordStream).getLines().toSet

  // Network stream
  val networkStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9000)
    .option("includeTimestamp", true) // necessary as eventTime-parameter for watermark
    .load()

  // Stream processing
  val words = networkStream.as[(String, Timestamp)]
    .flatMap(line => line._1.split("\\W+")
      .map(word => word.toLowerCase)
      .filter(word => word.length > 1 || word == "a" || word == "i")
      .filter(wordDict.contains(_))
      .map(word => (word, line._2)))
    .toDF("word", "timestamp")

  val windowedCounts = words
    .withWatermark("timestamp", "2 minutes") // late arrivals up to 2 mins allowed
    .groupBy("word")
    .count()

  val query = windowedCounts.writeStream
    .outputMode(OutputMode.Update) // watermarking demands Append- or Update-mode
    .format("console")
    .option("truncate", false)
    .start()

  // Stop processing with SIGINT
  query.awaitTermination()

}
