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
  * @create 2019-03-27 21:04 
  */
object TopThreeAd {
    //    每天各地区 top3 热门广告

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TopThreeAd")

        val streamContext = new StreamingContext(sparkConf, Seconds(3))

        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", streamContext)

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        streamContext.sparkContext.setCheckpointDir("cp")

        val updateStream: DStream[(String, Int)] = kafkaStream.map(record => {
            val datas: Array[String] = record.value().split(" ")
            AdClick(dateFormat.format(new Date(datas(0).toLong)), datas(1), datas(2), datas(3), datas(4))
        }).map(ad => {
            (ad.time + ":" + ad.area + ":" + ad.adid, 1)
        }).updateStateByKey {
            case (seq, cp) => {
                val sum: Int = cp.getOrElse(0) + seq.sum
                Option(sum)
            }
        }
        val result: DStream[(String, List[(String, Int)])] = updateStream.map {
            case (key, sum) => {
                val keys: Array[String] = key.split(":")
                (keys(0) + ":" + keys(1), (keys(2), sum))
            }
        }.groupByKey().mapValues(value => {
            value.toList.sortWith {
                case (left, right) => {
                    left._2 > right._2
                }
            }.take(3)
        })
        //        4.6 将结果保存到Redis中（json的转换）
        result.map {
            case (key, list) => {
                (key, list.toMap)
            }
        }.foreachRDD(rdd => {
            rdd.foreachPartition(datas => {
                val jedisClient: Jedis = RedisUtil.getJedisClient
                import org.json4s.JsonDSL._
                for ((key, value) <- datas) {
                    //(yyyy-MM-dd:area,(ad,sum))
                    val keys: Array[String] = key.split(":")
                    val jsonString: String = JsonMethods.compact(JsonMethods.render(value))
                    println(keys(0)+",:::"+keys(1)+",jsonString:"+jsonString)
                    jedisClient.hset("top3_ads_per_day:" + keys(0), keys(1), jsonString)
                }
                jedisClient.close()
            })
        })

        streamContext.start()
        streamContext.awaitTermination()
    }

}
