package com.atguigu.sparkmall

import java.text.SimpleDateFormat
import java.{lang, util}
import java.util.Date

import com.atguigu.sparkmall.common.util.{MyKafkaUtil, RedisUtil}
import com.atguigu.sparkmall.model.AdClick
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
  * @author star 
  * @create 2019-03-22 18:55 
  */
object AdBlackStateApp {

  def main(args: Array[String]): Unit = {
    //时间          区域  cityid  userid  adid
    //    1553252856544 华南 深圳     5       6
    //    1553252856544 华东 上海     6       1
    //    1553252856544 华南 广州     5       3
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AdBlackStateApp")

    val streamContext: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", streamContext)

    //set：存放黑名单

    //hash：存放点击次数
    /*kafkaStream.foreachRDD(rdd => {
      rdd.map(log => {
        val datas: Array[String] = log.value().split(" ")
        AdClick(datas(0), datas(1), datas(2), datas(3), datas(4))
      }).filter(ad => {
        val jedis: Jedis = RedisUtil.getJedisClient
        val flag: lang.Boolean = jedis.sismember("blacklist", ad.uid)
        jedis.close()
        !flag
      }).foreach(adv => {
        val date = new Date(adv.time.toLong)
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = RedisUtil.getJedisClient
        val count: lang.Long = jedis.hincrBy("userclickadcnt", dateFormat.format(date) + ":" + adv.uid + ":" + adv.adid, 1)
        if (count == 100) {
          jedis.sadd("blacklist", adv.uid)
        }
        jedis.close()
      })
    })*/

    val kafkaMessageStream: DStream[AdClick] = kafkaStream.map(record => {
      val datas: Array[String] = record.value().split(" ")

      AdClick(datas(0), datas(1), datas(2), datas(3), datas(4))
    })
    kafkaMessageStream.print()

    val transStream: DStream[AdClick] = kafkaMessageStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient
      val blackList: util.Set[String] = jedisClient.smembers("blacklist")
      jedisClient.close()
      print("blackList:"+blackList)
      //Spark中广播变量可以解决java序列化问题
      val broadCastBlackList: Broadcast[util.Set[String]] = streamContext.sparkContext.broadcast(blackList)

      print("broadCastBlackList:"+broadCastBlackList.value)
      rdd.filter(ad => {
        !broadCastBlackList.value.contains(ad.uid)
      })
    })
    print("======================75")
    transStream.print()
    transStream.foreachRDD(rdd=>{
      rdd.foreachPartition(datas=>{
        val client: Jedis = RedisUtil.getJedisClient
        datas.foreach(ad=>{
          val format = new SimpleDateFormat("yyyy-MM-dd")
          val field = format.format(new Date(ad.time.toLong))+":"+ad.uid+":" + ad.adid
          client.hincrBy("date:user:advert:clickcount",field,1)
          val sumClick: Long = client.hget("date:user:advert:clickcount",field).toLong

          if (sumClick>=100){
            client.sadd("blacklist",ad.uid)
          }
        })
        client.close()
      })
    })

    streamContext.start()
    streamContext.awaitTermination()
  }

}
