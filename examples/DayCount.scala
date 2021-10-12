// A distributed implementation of
// https://github.com/google/differential-privacy/tree/main/examples/java#count-visits-by-day-of-week

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, count}

object DayCount extends App {
  final private val DayDataCsv = "week_data.csv"
  final private val DayDataDelimiter = ","

  val spark:SparkSession = SparkSession.builder()
    .appName("DayCount")
    .getOrCreate()
  import spark.implicits._

  val epsilon = Math.log(3)
  val maxContributions = 3
  val maxContributionsPerPartition = 1

  val dataFrame = spark.read.option("delimiter", DayDataDelimiter).option("header", "true").csv(DayDataCsv)

  val privateCount = new PrivateCount[Long](epsilon, maxContributions)
  val privateCountUdf = functions.udaf(privateCount)

  // Restrict `maxContributionsPerPartition` of `VisitoryId` per `Day`, and `maxContributions` in total
  dataFrame.transform(BoundContribution("Day", "VisitorId", maxContributions, maxContributionsPerPartition)).show
    .groupBy("Day")
    .agg(privateCountUdf($"VisitorId") as "private_count", count($"VisitorId") as "non_private_count" )
    .show
}
