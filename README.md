# Task

Using Spark aggregate by `DISTRICT` following statistics (Boston Crime
dataset https://www.kaggle.com/AnalyzeBoston/crimes-in-boston):

- `crimes_total` - total crimes in `DISTRICT`
- `crimes_monthly` - median crimes count per month in `DISTRICT`
- `lat` - avg `Lat` of all crimes in `DISTRICT`
- `lng` - avg `Long` of all crimes in `DISTRICT`
- `frequent_crime_types` - top 3 most common crimes in `DISTRICT`. Join crime names using `,`. Order crimes from most
  frequent to low frequent. Use first part of `NAME` from table `offense_codes` (`BURGLARY - COMMERICAL` => `BURGLARY`)
  for define `crime_type`.

# Testing

```scala
package name.mrkandreev.common

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Start test suite with local spark context.
 */
trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  val SPARK_MASTER = "local[*]"

  val APP_NAME = "test"

  @transient private var _sc: SparkContext = _
  @transient private var _sqlContext: SparkSession = _

  def sc: SparkContext = _sc

  def sqlContext: SparkSession = _sqlContext

  var conf = new SparkConf(false)

  override def beforeAll() {
    _sc = new SparkContext(SPARK_MASTER, APP_NAME, conf)
    _sqlContext = SparkSession.builder.config(_sc.getConf).getOrCreate()
    super.beforeAll()
  }

  override def afterAll() {
    _sc.stop()
    super.afterAll()
  }
}
```

```scala
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

  // other test cases
}
```
