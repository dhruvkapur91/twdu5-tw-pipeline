package thoughtworks.wordcount

import WordCountUtils._
import thoughtworks.DefaultFeatureSpecWithSpark
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{QueryTest}


class WordCountUtilsTest extends DefaultFeatureSpecWithSpark with QueryTest with SharedSQLContext {
  import spark.implicits._

    feature("Split Words") {
    scenario("test splitting a dataset of words by spaces") {
      val exampleText = Seq("Hello here I am").toDS()
      assert(exampleText.splitWords(spark).collectAsList() === Seq("hello", "here", "i", "am").toDS.collectAsList())
    }

    scenario("test splitting a dataset of words by period") {
      val exampleText = Seq("Hello here I am. Hey.").toDS()
      assert(exampleText.splitWords(spark).collectAsList() === Seq("hello", "here", "i", "am", "hey").toDS.collectAsList())
    }

    scenario("test splitting a dataset of words by comma") {
      val exampleText = Seq("Hello here I am, hey").toDS()
      assert(exampleText.splitWords(spark).collectAsList() === Seq("hello", "here", "i", "am", "hey").toDS.collectAsList())
    }

    scenario("test splitting a dataset of words by hypen") {
      val exampleText = Seq("Hello-here-I-am").toDS()
      assert(exampleText.splitWords(spark).collectAsList() === Seq("hello", "here", "i", "am").toDS.collectAsList())
    }

    scenario("test splitting a dataset of words by semi-colon") {
      val exampleText = Seq("Hello here I am; Hey").toDS()
      assert(exampleText.splitWords(spark).collectAsList() === Seq("hello", "here", "i", "am", "hey").toDS.collectAsList())
    }
  }

  feature("Count Words") {
    scenario("basic test case") {
      import testImplicits._
      val exampleText = Seq("Hello here I am am").toDS()
      assert(exampleText.splitWords(spark).countByWord(spark).collect() === Seq(Array("hello", 1), Array("here", 1), Array("i", 1), Array("am", 2)).toDS.collect())
//      assert(exampleText.splitWords(spark).countByWord(spark).toDF === Seq(List("hello", 1), List("here", 1), List("i", 1), List("am", 2)).toDF)
      checkAnswer(exampleText.splitWords(spark).countByWord(spark).toDF(),
        Seq(("hello", 1), ("here", 1), ("i", 1), ("am", 2)).toDF())
    }

    scenario("should not aggregate dissimilar words") {
      import testImplicits._

      val exampleText = Seq("Hello here I am am").toDS()
      checkAnswer(exampleText.splitWords(spark).countByWord(spark).toDF(),
        Seq(("hello", 1), ("here", 1), ("i", 1), ("am", 2)).toDF())
    }

    ignore("test case insensitivity") {}
  }

  feature("Sort Words") {
    ignore("test ordering words") {}
  }

}
