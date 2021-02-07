package name.mrkandreev.bostoncrime

import name.mrkandreev.bostoncrime.BostonCrimeUtil._
import name.mrkandreev.common.SharedSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class BostonCrimeSuite extends AnyFunSuite with SharedSparkContext {
  test("extractCodeNameUdf") {
    val df = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("1", "first - second"),
          Row("2", "first"),
          Row("3", "")
        )
      ), new StructType()
        .add(StructField("CASE", StringType))
        .add(StructField("NAME", StringType))
    )

    val resultDf = df.withColumn("NAME", extractCodeNameUdf(col("NAME")))

    val ans = Map[String, String](("1", "first "), ("2", "first"), ("3", ""))
    resultDf.collect().foreach(row => {
      assert(ans.getOrElse(row.getAs[String]("CASE"), "?") == row.getAs[String]("NAME"))
    })
    assert(resultDf.count() == 3)
  }

  test("evaluateTotalCrimes") {
    val df = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a"),
          Row("a"),
          Row("b")
        )
      ), new StructType()
        .add(StructField("DISTRICT", StringType))
    )

    val resultDf = evaluateTotalCrimes(df)

    val ans = Map[String, Long](("a", 2L), ("b", 1L))
    resultDf.collect().foreach(row => {
      assert(ans.getOrElse(row.getAs[String]("DISTRICT"), -1L) == row.getAs[Long]("crimes_total"))
    })
    assert(resultDf.count() == 2)
  }

  test("evaluateMedianCrimesPerMonth") {
    val df = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", 2019, 1),
          Row("a", 2019, 1),
          Row("a", 2020, 3),
          Row("a", 2020, 3),
          Row("b", 2020, 1)
        )
      ), new StructType()
        .add(StructField("DISTRICT", StringType))
        .add(StructField("YEAR", IntegerType))
        .add(StructField("MONTH", IntegerType))
    )

    val resultDf = evaluateMedianCrimesPerMonth(df)

    val ans = Map[String, Long](("a", 2L), ("b", 1L))
    resultDf.collect().foreach(row => {
      assert(ans.getOrElse(row.getAs[String]("DISTRICT"), -1L) == row.getAs[Long]("crimes_monthly"))
    })
    assert(resultDf.count() == 2)
  }

  test("evaluateAvgLat") {
    val df = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", 40d),
          Row("a", 60d),
          Row("b", 41d)
        )
      ), new StructType()
        .add(StructField("DISTRICT", StringType))
        .add(StructField("Lat", DoubleType))
    )

    val resultDf = evaluateAvgLat(df)

    val ans = Map[String, Double](("a", 50d), ("b", 41L))
    resultDf.collect().foreach(row => {
      assert(ans.getOrElse(row.getAs[String]("DISTRICT"), -1d) == row.getAs[Double]("lat"))
    })
    assert(resultDf.count() == 2)
  }

  test("evaluateAvgLong") {
    val df = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", 40d),
          Row("a", 60d),
          Row("b", 41d)
        )
      ), new StructType()
        .add(StructField("DISTRICT", StringType))
        .add(StructField("Long", DoubleType))
    )

    val resultDf = evaluateAvgLong(df)

    val ans = Map[String, Double](("a", 50d), ("b", 41L))
    resultDf.collect().foreach(row => {
      assert(ans.getOrElse(row.getAs[String]("DISTRICT"), -1d) == row.getAs[Double]("lng"))
    })
    assert(resultDf.count() == 2)
  }

  test("evaluateTopCrimes") {
    val df = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("a", "1"),
          Row("a", "1"),
          Row("a", "2"),
          Row("b", "1")
        )
      ), new StructType()
        .add(StructField("DISTRICT", StringType))
        .add(StructField("OFFENSE_CODE", StringType))
    )
    val dfCodes = sqlContext.createDataFrame(
      sc.parallelize(
        Seq(
          Row("1", "CODE A"),
          Row("2", "CODE B"),
        )
      ), new StructType()
        .add(StructField("OFFENSE_CODE", StringType))
        .add(StructField("NAME", StringType))
    )

    val resultDf = evaluateTopCrimes(df, dfCodes, topN = 3)
    resultDf.show(5)

    val ans = Map[String, String](("a", "CODE A, CODE B"), ("b", "CODE A"))
    resultDf.collect().foreach(row => {
      assert(ans.getOrElse(row.getAs[String]("DISTRICT"), "?") == row.getAs[String]("frequent_crime_types"))
    })
    assert(resultDf.count() == 2)
  }
}
