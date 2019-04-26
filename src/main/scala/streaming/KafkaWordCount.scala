package streaming

import java.util
import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.Interval
import org.apache.spark.streaming.kafka._

object KafkaWordCount {
  implicit val formats = DefaultFormats
  def main(args:Array[String]){
    if(args.length < 4){
      //zookeeper地址，消息所在组，消费者所消费的topics，开启消费topics线程个数
      System.err.println("Usage:KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    StreamingExamples.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint(".")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //创建kafka连接 返回上文中设置的一秒内获得的数据量
    val lines =  KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))//使用空格分割（默认的就是空格）

    val wordCounts = words.map(x => (x, 1L)).reduceByKeyAndWindow(_+_, _-_, Seconds(1), Seconds(1), 1).foreachRDD(
      rdd =>{
        if(rdd.count !=0){
          val props = new util.HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092")
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
          //实例化kafka对象
          val producer = new KafkaProducer[String,String](props)
          // rdd.colect即将rdd中数据转化为数组，然后write函数将rdd内容转化为json格式
          val str = write(rdd.collect())
          //tocpic 为 result
          val message = new ProducerRecord[String,String]("result",null,str)

          producer.send(message)
        }
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
}     