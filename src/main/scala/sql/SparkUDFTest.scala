package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @author smi1e
  *         Date 2019/10/27 9:38
  *         Description 自定义聚合函数
  */
object SparkUDFTest {
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
    peopleDF.createTempView("people")
    spark.udf.register("MyAvg", new MyAvg)
    spark.sql("select MyAvg(age) from people").show()
    spark.stop()
  }

  class MyAvg extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      new StructType().add("sum", LongType)
    }

    override def bufferSchema: StructType = {
      new StructType().add("sum", LongType).add("count", LongType)
    }

    override def dataType: DataType = {
      DoubleType
    }

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }

}
