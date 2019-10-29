package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description glom
  */
object SparkTest05 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)
    val glomRDD: Array[Array[Int]] = listRDD.glom().collect()
    for (elem <- glomRDD) {
      println(elem.mkString)
    }


    sc.stop()
  }

}
