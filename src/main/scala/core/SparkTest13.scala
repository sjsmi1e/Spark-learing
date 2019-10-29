package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description groupByKey-----WordCount
  */
object SparkTest13 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    listRDD.aggregateByKey(0)((x, y) => {
      math.max(x, y)
    }, (x, y) => {
      x + y
    }).foreach(println)

    sc.stop()
  }

}
