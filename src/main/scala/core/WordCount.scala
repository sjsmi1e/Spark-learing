package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/21 19:59
  *         Description
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("WC")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.textFile("input").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)
//    val fileRdd: RDD[String] = sc.textFile("input",2)
//    fileRdd.saveAsTextFile("output")
//    val value: RDD[Int] = sc.makeRDD(Array(1,2),8)
//    value.saveAsTextFile("output")
    //4.关闭连接
    sc.stop()
  }
}
