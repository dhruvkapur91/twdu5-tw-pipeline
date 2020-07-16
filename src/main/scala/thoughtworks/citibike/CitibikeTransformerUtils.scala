package thoughtworks.citibike

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}

object CitibikeTransformerUtils {
  private final val MetersPerFoot = 0.3048
  private final val FeetPerMile = 5280

  final val EarthRadiusInM: Double = 6371e3
  final val MetersPerMile: Double = MetersPerFoot * FeetPerMile

  implicit class StringDataset(val dataSet: Dataset[Row]) {

    def computeDistances(spark: SparkSession) = {
      dataSet.withColumn("distance",
        lit(roundFunc(haversineFunc(
          col("start_station_longitude"),
          col("start_station_latitude"),
          col("end_station_longitude"),
          col("end_station_latitude"),
          lit(EarthRadiusInM))/lit(MetersPerFoot)/lit(FeetPerMile),lit(2))
      ))
    }


    val roundValue = (d: Double, p: Integer) => math.BigDecimal(d).setScale(p, BigDecimal.RoundingMode.HALF_UP).toDouble
    val roundFunc = udf(roundValue)

    val haversine = (startLon: Double, startLat: Double, endLon: Double, endLat: Double, radius: Double) => {
      val dLat = math.toRadians(endLat - startLat)
      val dLon = math.toRadians(endLon - startLon)
      val lat1 = math.toRadians(startLat)
      val lat2 = math.toRadians(endLat)

      val a = math.sin(dLat / 2) * math.sin(dLat / 2) +
        math.sin(dLon / 2) * math.sin(dLon / 2) * math.cos(lat1) * math.cos(lat2)
      val c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

      radius * c
    }

    val haversineFunc = udf(haversine)

  }
}
