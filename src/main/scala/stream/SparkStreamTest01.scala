package stream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author smi1e
  *         Date 2019/10/28 15:59
  *         Description 
  */
object SparkStreamTest01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreamTest01")
    val streamingContext = new StreamingContext(conf, Seconds(3))
    streamingContext.checkpoint("streamCheck")
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    //自定义接收器
    //    val line: ReceiverInputDStream[String] = streamingContext.receiverStream(new MyReceiver("localhost", 9999))
    val line: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
    val value: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1))
    //永久记录
//    value.updateStateByKey(updateFunc).print()
    //滑动窗口
    value.reduceByKey(_ + _).window(Seconds(9),Seconds(6)).print()


    //启动SparkStreamingContext
    streamingContext.start()
    streamingContext.awaitTermination()

  }

  class MyReceiver(host: String, port: Int) extends Receiver[String](storageLevel = StorageLevel.MEMORY_ONLY) {

    override def onStart(): Unit = {
      new Thread("MyReceiver") {
        override def run() {
          receive()
        }
      }.start()
    }

    def receive(): Unit = {
      val socket = new Socket(host, port)
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
      var line: String = null
      while (!"END".equals(line = reader.readLine())) {
        //        println(line)
        store(line)
      }
      reader.close()
      socket.close()
      restart("restart")
    }

    override def onStop(): Unit = {

    }
  }

}
