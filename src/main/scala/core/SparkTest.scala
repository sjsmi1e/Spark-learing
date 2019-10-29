package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description coalesce重新定义分区
  */
object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRdd: RDD[Int] = sc.makeRDD(1 to 16)
    listRdd.saveAsTextFile("output1")
    val coalesceRdd: RDD[Int] = listRdd.coalesce(2)
    coalesceRdd.saveAsTextFile("output2")
    sc.stop()
  }

}
