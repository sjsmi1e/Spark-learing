package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description map函数,所有元素*2
  */
object SparkTest02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRdd: RDD[Int] = sc.makeRDD(1 to 16)

    for (elem <- listRdd.map(x => x * 2).collect()) {
      println(elem)
    }

    sc.stop()
  }

}
