package thoughtworks.wordcount

import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{Dataset, SparkSession}

object WordCountUtils {

  implicit class StringDataset(val dataSet: Dataset[String]) {
    def splitWords(spark: SparkSession) = {
      import spark.implicits._

      dataSet.flatMap(x => x.split("[;,-.\\s+]").map(x => x.trim().toLowerCase)).filter(_.nonEmpty)
    }

    def countByWord(spark: SparkSession) = {
//      dataSet.as[String]
      dataSet.groupBy("value").agg(count("value"))


    }
  }
}
