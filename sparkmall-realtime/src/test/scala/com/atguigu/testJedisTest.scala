package com.atguigu

import redis.clients.jedis.Jedis

/**
  * @author star 
  * @create 2019-04-18 14:44 
  */
object testJedisTest {

    def main(args: Array[String]): Unit = {
        val jedis: Jedis = new Jedis("hadoop59")
        println(jedis.ping())
    }
}
