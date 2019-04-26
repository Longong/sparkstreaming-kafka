package demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

object KafkaWordCount{
  def main(args:Array[String]){
    StreamingExamples.setStreamingLogLevels()
    val sc = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sc,Seconds(10))
    ssc.checkpoint("file:///usr/local/spark/mycode/kafka/checkpoint") //设置检查点
    val zkQuorum = "localhost:2181" //Zookeeper服务器地址
    val group = "1"  //topic所在的group
    val topics = "wordsender"  //topics的名称
    val numThreads = 1  //每个topic的分区数
    val topicMap =topics.split(",").map((_,numThreads.toInt)).toMap
    val lineMap = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
    val lines = lineMap.map(_._2)
    val words = lines.flatMap(_.split(" "))
    val pair = words.map(x => (x,1))
    val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
    wordCounts.print
    ssc.start
    ssc.awaitTermination
  }
}     
