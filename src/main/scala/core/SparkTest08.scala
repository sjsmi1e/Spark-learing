package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description 创建一个RDD（1-10），从中选择放回和不放回抽样
  */
object SparkTest08 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //放回抽样
    val sampleRDD1: RDD[Int] = listRDD.sample(true, 0.4, 1)
    println("----------放回抽样----------")
    sampleRDD1.foreach(println)
    //不放回抽样
    val sampleRDD2: RDD[Int] = listRDD.sample(false, 0.4, 1)
    println("----------不放回抽样----------")
    sampleRDD2.foreach(println)
    sc.stop()
  }

}
