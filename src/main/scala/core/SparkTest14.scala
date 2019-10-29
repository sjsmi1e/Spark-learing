package core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description 累加器
  */
object SparkTest14 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val accumulator: LongAccumulator = sc.longAccumulator
    var sum = 0
    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    listRDD.foreach(x => {
      accumulator.add(1)
      sum = sum + 1
    })

    println(accumulator.value)
    println(sum)
    sc.stop()
  }

}
