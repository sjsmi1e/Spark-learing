package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description 创建一个RDD，按照不同的规则进行排序
  */
object SparkTest10 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16 ,4)
    listRDD.sortBy(x=>x).foreach(println)
    sc.stop()
  }

}
