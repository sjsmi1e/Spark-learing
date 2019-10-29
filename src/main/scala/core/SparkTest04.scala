package core
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
/**
  * @author smi1e
  *         Date 2019/10/22 15:45
  *         Description mapPartitionsWithIndex,所有元素与分区数组成元组返回新的RDD
  */
object SparkTest04 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("ST")
    val sc = new SparkContext(conf)
    val listRdd: RDD[Int] = sc.makeRDD(1 to 16)

    listRdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        datas.map((index,_))
      }
    }.foreach(println)

    sc.stop()
  }

}
