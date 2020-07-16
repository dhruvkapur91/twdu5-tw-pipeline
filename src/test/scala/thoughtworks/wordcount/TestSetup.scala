package thoughtworks.wordcount

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.scalatest.FunSuiteLike


class TestSetup extends QueryTest with FunSuiteLike with SharedSQLContext {

  override val spark: SparkSession = SparkSession.builder
    .appName("Spark Test App")
    .config("spark.driver.host", "127.0.0.1")
    .master("local")
    .getOrCreate()

  test("hello") {
    1 + 1
  }


}
