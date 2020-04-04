package com.atguigu.sparkmall

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkmall.model.AdClick
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author star 
  * @create 2019-03-27 19:24 
  */
object ClickSumstateApp {
    //  每天各地区各城市各广告的点击流量实时统计
    //case class AdClick(time:String,area:String,cid:String,uid:String,adid:String)
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ClickSumstateApp")

        val streamContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", streamContext)

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")

        val adStream: DStream[AdClick] = kafkaStream.map(record => {
            val datas: Array[String] = record.value().split(" ")
            AdClick(dateFormat.format(new Date(datas(0).toLong)), datas(1), datas(2), datas(3), datas(4))
        })

        val reduceStream: DStream[(String, Int)] = adStream.map(ad => {
            (ad.time + ":" + ad.area + ":" + ad.cid + ":" + ad.adid, 1)
        }).reduceByKey(_ + _)

        /*
        //推荐版本
        reduceStream.foreachRDD(rdd=>{
            rdd.foreachPartition(datas=>{
                val jedisClient: Jedis = RedisUtil.getJedisClient
                for((key,sum)<- datas) {
                    jedisClient.hincrBy("date:area:city:ads",key,sum)
                }
            })
        })*/

        streamContext.sparkContext.setCheckpointDir("cp")

        val updateStream: DStream[(String, Long)] = reduceStream.updateStateByKey {
            case (seq, cp) => {
                val sum: Long = cp.getOrElse(0l) + seq.sum
                Option(sum)
            }
        }

        updateStream.print()
        updateStream.foreachRDD(rdd => {
            rdd.foreachPartition(datas => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                for ((key, sum) <- datas)
                    jedisClient.hset("date:area:city:ads", key, sum.toString)
                jedisClient.close()
            })
        })


        streamContext.start()

        streamContext.awaitTermination()
    }
}
