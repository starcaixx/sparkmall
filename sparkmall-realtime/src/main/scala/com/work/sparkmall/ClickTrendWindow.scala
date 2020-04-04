package com.atguigu.sparkmall

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkmall.model.AdClick
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.JsonMethods
import redis.clients.jedis.Jedis

/**
  * @author star 
  * @create 2019-03-28 10:29 
  */
object ClickTrendWindow {

    def main(args: Array[String]): Unit = {
        //        1553739805849 华南 深圳 5 2
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClickTrendWindow")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))
        //一小时内广告点击量趋势图
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", streamingContext)
        val dateFormat = new SimpleDateFormat("mm:ss")

        val resultStream: DStream[(String, List[(String, Int)])] = kafkaStream.map(record => {
            val datas: Array[String] = record.value().split(" ")
            AdClick(datas(0), datas(1), datas(2), datas(3), datas(4))
        }).window(Seconds(60), Seconds(10)).map(ad => {
            (ad.time.toLong / 10000 + "0000:" + ad.adid, 1)
        }).reduceByKey(_ + _).map {
            case (key, sum) => {
                val keys: Array[String] = key.split(":")
                (keys(1), (dateFormat.format(new Date(keys(0).toLong)), sum))
            }
        }.groupByKey().mapValues(datas => {
            datas.toList.sortWith {
                case (left, right) => {
                    left._1 < right._1
                }
            }
        })
        resultStream.print()
        import org.json4s.JsonDSL._
        resultStream.foreachRDD(rdd=>{
            rdd.foreachPartition(datas=>{
                val jedisClient: Jedis = RedisUtil.getJedisClient
                for((key,list)<-datas) {
                    val jsonString: String = JsonMethods.compact(JsonMethods.render(list))
                    jedisClient.hset("time:advert:click:trend",key,jsonString)
                }
                jedisClient.close()
            })
        })


        streamingContext.start()
        streamingContext.awaitTermination()
    }
}
