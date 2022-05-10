package name.mrkandreev.bostoncrime

import com.typesafe.config.{Config, ConfigFactory}
import name.mrkandreev.bostoncrime.BostonCrimeUtil._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object BostonCrimeApp {
  final val conf: Config = ConfigFactory.load
  final val master: String = conf.getString("master")
  final val dataFile: String = conf.getString("dataFile")
  final val dictFile: String = conf.getString("dictFile")
  final val outputPath: String = conf.getString("outputPath")
  final val topCrimesCount = conf.getInt("topCrimesCount")

  def main(args: Array[String]): Unit = {
    val sqlContext: SparkSession = SparkSession.builder
      .appName("BostonCrime Application")
      .master(master)
      .getOrCreate()

    val df = sqlContext
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(dataFile)
      .filter("DISTRICT is not null")

    val dfCodes = sqlContext
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load(dictFile)
      .withColumn("NAME", extractCodeNameUdf(col("NAME")))
      .withColumnRenamed("CODE", "OFFENSE_CODE")
      .filter(expr("OFFENSE_CODE is not null"))

    df.select("DISTRICT").distinct()
      .join(evaluateTotalCrimes(df), "DISTRICT")
      .join(evaluateMedianCrimesPerMonth(df), "DISTRICT")
      .join(evaluateTopCrimes(df, dfCodes, topCrimesCount), "DISTRICT")
      .join(evaluateAvgLat(df), "DISTRICT")
      .join(evaluateAvgLong(df), "DISTRICT")
      .write
      .format("csv")
      .option("header", "true")
      .option("sep", ",")
      .mode("overwrite")
      .save(outputPath)

    sqlContext.stop()
  }
}
