package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description 创建一个RDD（1-10），从中选择放回和不放回抽样
  */
object SparkTest09 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,1,5,2,9,6,1))
    listRDD.distinct().saveAsTextFile("output1")
    listRDD.distinct(3).saveAsTextFile("output2")
    sc.stop()
  }

}
