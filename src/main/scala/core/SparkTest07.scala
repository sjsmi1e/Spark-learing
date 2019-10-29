package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description 创建一个RDD（由字符串组成），过滤出一个新RDD（包含”xiao”子串）
  */
object SparkTest07 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRDD: RDD[String] = sc.makeRDD(Array("xiaoming","xiaojiang","xiaohe","dazhi"))
    val resRDD: Array[String] = listRDD.filter(_.contains("xiao")).collect()
    resRDD.foreach(println)
    sc.stop()
  }

}
