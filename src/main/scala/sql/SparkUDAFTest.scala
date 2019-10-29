package sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}


/**
  * @author smi1e
  *         Date 2019/10/27 9:38
  *         Description 自定义聚合函数（强类型）
  */
object SparkUDAFTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val peopleDF: DataFrame = spark.sparkContext.textFile("input/people.txt").map(x => {
      val fields: Array[String] = x.split(",")
      (fields(0), fields(1).toLong)
    }).toDF("name", "age")
    peopleDF.show()
    peopleDF.select("age")

    val column: TypedColumn[People, Double] = new MyAvg().toColumn.name("avg")
    val peopleDS: Dataset[People] = peopleDF.as[People]
    peopleDS.select(column).show()

    spark.stop()
  }

  case class People(var name: String, var age: BigInt)

  case class PeopleBuf(var sum: BigInt, var count: Long)


  class MyAvg extends Aggregator[People, PeopleBuf, Double] {
    override def zero: PeopleBuf = PeopleBuf(0L, 0L)

    override def reduce(b: PeopleBuf, a: People): PeopleBuf = {
      b.count = b.count + 1
      b.sum = b.sum + a.age

      b
    }

    override def merge(b1: PeopleBuf, b2: PeopleBuf): PeopleBuf = {
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: PeopleBuf): Double = {
      reduction.sum.toDouble / reduction.count
    }

    override def bufferEncoder: Encoder[PeopleBuf] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }

}
