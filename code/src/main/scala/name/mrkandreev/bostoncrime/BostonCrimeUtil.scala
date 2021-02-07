package name.mrkandreev.bostoncrime

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object BostonCrimeUtil {
  val extractCodeNameUdf: UserDefinedFunction = udf((x: String) => x.split("-")(0))

  def evaluateTotalCrimes(df: DataFrame): DataFrame = {
    df
      .groupBy("DISTRICT")
      .count().withColumnRenamed("count", "crimes_total")
  }

  def evaluateMedianCrimesPerMonth(df: DataFrame): DataFrame = {
    df
      .withColumn("YEAR_MONTH", concat_ws("-", df("YEAR"), df("MONTH")))
      .groupBy("DISTRICT", "YEAR_MONTH")
      .count()
      .groupBy("DISTRICT")
      .agg(expr("approx_percentile(count, 0.5)").as("crimes_monthly"))
  }

  def evaluateAvgLat(df: DataFrame): DataFrame = {
    df
      .filter(expr("Lat is not null"))
      .groupBy("DISTRICT")
      .agg(expr("avg(Lat)").as("lat"))
  }

  def evaluateAvgLong(df: DataFrame): DataFrame = {
    df
      .filter(expr("Long is not null"))
      .groupBy("DISTRICT")
      .agg(expr("avg(Long)").as("lng"))
  }

  def evaluateTopCrimes(df: DataFrame, dfCodes: DataFrame, topN: Int): DataFrame = {
    val window = Window.partitionBy("DISTRICT").orderBy(desc("count"))

    df
      .filter(expr("OFFENSE_CODE is not null"))
      .withColumn("OFFENSE_CODE", df("OFFENSE_CODE").cast(IntegerType))
      .groupBy("DISTRICT", "OFFENSE_CODE")
      .count()
      .withColumn("rank", row_number().over(window))
      .sort(desc("count"))
      .where(col("rank") <= topN)
      .join(
        dfCodes
          .withColumn("OFFENSE_CODE", dfCodes("OFFENSE_CODE").cast(IntegerType)),
        "OFFENSE_CODE"
      )
      .select("DISTRICT", "NAME")
      .groupBy("DISTRICT")
      .agg(concat_ws(", ", collect_list("NAME")) as "frequent_crime_types")
      .select("DISTRICT", "frequent_crime_types")
  }
}
