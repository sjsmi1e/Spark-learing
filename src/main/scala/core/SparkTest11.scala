package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description partitionBy
  */
object SparkTest11 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)

    val listRDD: RDD[(Int, String)] = sc.makeRDD(Array((1,"aaa"),(2,"bbb"),(3,"ccc"),(4,"ddd")),4)
    println(listRDD.partitions.size)
    listRDD.glom().collect().foreach(x=>{
      println(x.mkString)
    })
    val partitionRDD: RDD[(Int, String)] = listRDD.partitionBy(new org.apache.spark.HashPartitioner(2))
    println(partitionRDD.partitions.size)
    partitionRDD.glom().collect().foreach(x=>{
      println(x.mkString)
    })

    sc.stop()
  }

}
