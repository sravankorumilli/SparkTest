package demo

import org.apache.spark.rdd.RDD

class DemoTest extends SparkTestCase{
    test("should be able to duplicate data of given window size to previous partition") {
        val data = Array("1", "2", "3")
        val dataSet: RDD[String] = sparkContext.parallelize(data)
        val collect: Array[String] = dataSet.collect()
        assert(3 == collect.length)
    }
}
