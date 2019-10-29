package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description groupBy按值模2分组
  */
object SparkTest06 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRDD: RDD[Int] = sc.makeRDD(1 to 16)
    val groupRDD: Array[(Int, Iterable[Int])] = listRDD.groupBy(_%2).collect()
    groupRDD.foreach(println)
    sc.stop()
  }

}
