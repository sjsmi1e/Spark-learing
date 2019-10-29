package core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author smi1e
  *         Date 2019/10/23 11:14
  *         Description
  */
object Practice {
  def main(args: Array[String]): Unit = {
//    val args = new Array[String](2)
//    args(0) = "input"
//    args(1) = "output"
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local[*]").setAppName("Practice")
    val sc = new SparkContext(conf)
    //读取数据
    val line: RDD[String] = sc.textFile(args(0))
    //" "分割，以省份和广告号为key
    val practie: RDD[((String, String), Int)] = line.map(x => {
      val fields: Array[String] = x.split(" ")
      ((fields(1), fields(4)), 1)
    })
    //求和
    val praceSum: RDD[((String, String), Int)] = practie.reduceByKey(_ + _)
    //转化为省份key
    val tempSum: RDD[(String, (String, Int))] = praceSum.map(x => {
      (x._1._1, (x._1._2, x._2))
    })
    val res: RDD[(String, List[(String, Int)])] = tempSum.groupByKey().mapValues(x => {
      x.toList.sortBy(-_._2).take(3)
    })
    res.saveAsTextFile(args(1))
    sc.stop()
  }
}
