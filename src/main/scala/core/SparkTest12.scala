package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description groupByKey-----WordCount
  */
object SparkTest12 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)

    val newRDD: RDD[(String, Int)] = sc.makeRDD(Array("one", "two", "two", "three", "three", "three")).map((_, 1))
    newRDD.groupByKey().map(x=>{
      x._1+x._2.sum
    }).foreach(println)


    sc.stop()
  }

}
