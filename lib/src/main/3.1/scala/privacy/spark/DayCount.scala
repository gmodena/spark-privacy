package privacy.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count}

object DayCount extends App {
  final private val DayDataCsv = "day_data.csv"
  final private val DayDataDelimiter = ","

  val spark:SparkSession = SparkSession.builder()
   .appName("DayCount")
   .getOrCreate()

  val epsilon = Math.log(3)
  val maxContributions = 5

  val dataFrame = spark.read
    .option("delimiter", DayDataDelimiter)
    .option("header", "true")
    .csv(DayDataCsv)

  val privateCount = new PrivateCount(epsilon, maxContributions)

  // Restrict `maxContributions` of `VisitoryId` per `Day`.
  dataFrame.transform(BoundContribution("Day", "VisitorId", maxContributions))
    .groupBy("Day")
    .agg(count(col("Day")), privateCount.toColumn.name("private_count"))
    .show
}
