import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "src/main/resources/README.md"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).repartition(2).cache()
    wordCount(logData)
  }

  def wordCount(textFile : RDD[String]): Unit ={
    val wordCounts: RDD[(String, Int)] = textFile.flatMap(_.split(" ").map(word => {(word, 1)})).reduceByKey((a, b)=>{a+b})
    val counts: Array[(String, Int)] = wordCounts.collect()
    println("All Word Counts :")
    counts.foreach(tuple=>{println(s"${tuple._1}:${tuple._2}")})
  }

}