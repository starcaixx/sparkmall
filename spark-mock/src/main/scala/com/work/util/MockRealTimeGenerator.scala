package com.work.util

import java.util.concurrent.TimeUnit
import java.util.{Properties, Random}

import com.work.sparkmall.common.util.ConfigurationUtil
import com.atguigu.sparkmall.model.CityInfo
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer

/**
  * @author star 
  */
object MockRealTimeGenerator {

  def generateMockData: Array[String] = {
    val arry: ArrayBuffer[String] = ArrayBuffer[String]()

    val cityRandomOpt = RandomOptions(RanOpt(CityInfo(1, "北京", "华北"), 30),
      RanOpt(CityInfo(1, "上海", "华东"), 30),
      RanOpt(CityInfo(1, "广州", "华南"), 10),
      RanOpt(CityInfo(1, "深圳", "华南"), 20),
      RanOpt(CityInfo(1, "天津", "华北"), 10))

    val random = new Random()

    for(i<- 0 to 50){
      val timestamp = System.currentTimeMillis()
      val cityInfo = cityRandomOpt.getRandomOpt()
      val city = cityInfo.city_name
      val area = cityInfo.area
      val adid = 1+random.nextInt(6)
      val userid = 1+random.nextInt(6)

      // 拼接实时数据
      arry += timestamp + " " + area + " " + city + " " + userid + " " + adid

    }
    arry.toArray
  }

  def createKafkaProducer(kafkaList: String): KafkaProducer[String,String] = {
    val prop = new Properties()

    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaList)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String,String](prop)
  }

  def main(args: Array[String]): Unit = {
    //get kafka config
    val kafkaList: String = ConfigurationUtil.getValueByKey("kafka.broker.list")

    val topic = "ads_log"

    //producer
    val kfk: KafkaProducer[String, String] = createKafkaProducer(kafkaList)

    while(true) {
      //random produce data and send kafkas by producer
      for (line <- generateMockData){
//        kfk.send(new ProducerRecord[String,String](topic,line))
        println(line)
      }
      TimeUnit.MICROSECONDS.sleep(5000)
    }

  }
}
