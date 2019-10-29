package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author smi1e
  *         Date 2019/10/27 9:38
  *         Description 
  */
object SparkSqlTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//    spark.read.text("input/people.txt")
    val sc: SparkContext = spark.sparkContext
    val peopleRDD: RDD[(String, Int)] = sc.textFile("input/people.txt").map(x => {
      val fields: Array[String] = x.split(",")
      (fields(0), fields(1).trim.toInt)
    })
    import spark.implicits._
    //rdd转df
    val peopleDF: DataFrame = peopleRDD.toDF("name","age")
    //df查询
    println("----------df查询----------------")
    peopleDF.select($"name",$"age"+1).show()
    println("----------ds查询----------------")
    val peopleDS: Dataset[people] = peopleDF.as[people]
    peopleDS.select($"name",$"age"+1).show()
    sc.stop()
    spark.stop()
  }
  case class people(name: String,age:Int)

}
