package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author smi1e
  *         Date 2019/10/28 15:09
  *         Description
  */
object SparkDataTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkDataTest")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //从mysql中加载数据
//    val jdbcDF: DataFrame = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/user?characterEncoding=utf8&serverTimezone=GMT%2B8")
//      .option("dbtable", "user")
//      .option("user", "root")
//      .option("password", "789396")
//      .load()
//    jdbcDF.show()
    import spark.implicits._
    val peopleDF: DataFrame = spark.sparkContext.textFile("input/people.txt").map(x => {
      val fields: Array[String] = x.split(",")
      (fields(0), fields(1).trim.toInt)
    }).toDF("name", "age")
    peopleDF.show()
    //写入数据库
    peopleDF.write.format("jdbc").mode("append")
      .option("url", "jdbc:mysql://localhost:3306/user?characterEncoding=utf8&serverTimezone=GMT%2B8")
      .option("dbtable", "people")
      .option("user", "root")
      .option("password", "789396")
      .save()

  }
  case class People(name:String,age:Int)
}

