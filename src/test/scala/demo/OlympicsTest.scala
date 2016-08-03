package demo

import org.apache.spark.rdd.RDD
import spark.workshop.Olympics

class OlympicsTest extends SparkTestCase {

  test("should find total medals won by country for a given year") {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val totalMedals: Int = Olympics(inputRDD).totalMedalsWon("United States",2002)

    assertResult(17)(totalMedals)
  }

  test("should find year of max medals for a given country") {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val actualYear: Int = Olympics(inputRDD).findYearOfMaxMedals("United States")

    assertResult(2004)(actualYear)
  }

  test("should find players with medals in more than one sport") {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val players: Array[(String, String)] = Olympics(inputRDD).playersWithMedalsInMoreThanOneSport(2000)

    val expectedPlayers = Array(("Li Na", "China"))
    assert(players.length == 1)
    assert(players.contains(expectedPlayers(0)))
  }

  test("should find which country got max medals in a given year") {
    val olympicsData = "src/main/resources/olympic.csv"
    val inputRDD: RDD[String] = sparkContext.textFile(olympicsData)

    val country: String = Olympics(inputRDD).countryWithMaxMedalsFor(2002)

    assertResult("United States")(country)
  }
}
