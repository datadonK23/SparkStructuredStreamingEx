import org.apache.spark.sql.functions._
import org.apache.spark.sql._

import java.sql.Timestamp

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

  // Network stream
  val networkStream = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9000)
    .option("includeTimestamp", true)
    .load()

  // Stream processing
  val words = networkStream.as[(String, Timestamp)]
    .flatMap(line => line._1.split("\\W+").map(word => (word, line._2)))
    .toDF("word", "timestamp")

  val windowedCounts = words.groupBy(
    window($"timestamp", "10 minutes", "5 minutes"),
    $"word")
    .count()
  //FIXME watermark

  val query = windowedCounts.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", false)
    .start()

  // Stop processing with SIGINT
  query.awaitTermination()

}
