package com.work.sparkmall

import java.util.UUID

import com.work.sparkmall.common.util.ConfigurationUtil
import com.work.sparkmall.model.UserVisitAction
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable

/**
  * @author star
  */
object PageJumpConvertApp {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageJumpConvertApp")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import sparkSession.implicits._
    //change database
    sparkSession.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))

    //sql
    val resultDF: DataFrame = sparkSession.sql("select * from user_visit_action")

    val usersRDD: RDD[UserVisitAction] = resultDF.as[UserVisitAction].rdd

    /*
    date								2018-11-26
    user_id             38
    session_id          0ce68795-7777-4534-97d5-b998dfb679af
    page_id             45
    action_time         2018-11-26 00:00:00
    search_keyword      NULL
    click_category_id   -1
    click_product_id    -1
    order_category_ids  NULL
    order_product_ids   NULL
    pay_category_ids    13,17,11,14
    pay_product_ids     10,14,17
    city_id             7
    */
    //(sid,user)=>(pid,(1,2,3,4,5,6))=>()

    val filterCondition: String = ConfigurationUtil.getCondValue("targetPageFlow")
    val condition: Array[String] = filterCondition.split(",")
    val conditionfilter: Array[String] = condition.zip(condition.tail).map(f => (f._1 + "-" + f._2))

    //(pid-pid,1)
    val flowResult: RDD[(String, List[(String, Int)])] = usersRDD.groupBy(_.session_id).mapValues {
      case users => {
        val pids: List[Long] = users.toList.sortWith(_.action_time < _.action_time).map(user => user.page_id)
        val pidflow: List[(Long, Long)] = pids.zip(pids.tail)
        pidflow.filter(p => conditionfilter.contains(p._1 + "-" + p._2)).map(f => (f._1 + "-" + f._2, 1))
      }
    }
    //(1-2,2)
    val result: RDD[(String, Int)] = flowResult.flatMap(x => x._2).reduceByKey(_ + _)

    //(user)
    val sumResult: RDD[(Long, Int)] = usersRDD.filter(user => {
      condition.contains(user.page_id + "")
    }).map(user => (user.page_id, 1)).reduceByKey(_ + _)

    val mapSum: Map[Long, Int] = sumResult.collect().toMap

    sumResult.foreach(println)
    println("====================")

    result.foreach { datas => {
      val pid: Array[String] = datas._1.split("-")
      println("++++++++++++")
      println(mapSum.get(pid(0).toLong))
      println("========")
      println(datas._2.toDouble / mapSum.get(pid(0).toLong).get)
    }
    }

    sparkSession.stop()
    /*// 4.2 将日志数据根据session进行分组排序，获取同一个session中的页面跳转路径
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = usersRDD.groupBy(log=>{log.session_id})

    // 4.3 将页面跳转路径形成拉链效果  AB.BC
    val zipPageidCountRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(datas => {
      val pageFlows: List[UserVisitAction] = datas.toList.sortWith {
        case (left, right) => {
          left.action_time < right.action_time
        }
      }
      // 1 - 2 -3 - 4 - 5
      // 2 - 3- 4- 5
      val pageidFlows: List[Long] = pageFlows.map(action => action.page_id)
      // (1-2), (2-3)，(3-4), (4-5)
      val zipPageids: List[(Long, Long)] = pageidFlows.zip(pageidFlows.tail)
      zipPageids.map {
        case (pid1, pid2) => {
          (pid1 + "_" + pid2, 1)
        }
      }
    })
    // 4.4 统计拉链后的数据点击总次数（A）
    val flatZipToCountRDD: RDD[(String, Int)] = zipPageidCountRDD.map(_._2).flatMap(x=>x)

    // 获取条件参数
    val targetIds: Array[String] = ConfigurationUtil.getCondValue("targetPageFlow").split(",")
    // (1-2), (2-3),(3-4)
    val targetPageFlows: Array[String] = targetIds.zip(targetIds.tail).map {
      case (pid1, pid2) => {
        pid1 + "_" + pid2
      }
    }

    val filterZipToCountRDD: RDD[(String, Int)] = flatZipToCountRDD.filter {
      // pageflow => (1-2), (2-5)
      case (pageflow, count) => {
        targetPageFlows.contains(pageflow)
      }
    }

    // (1-2, 10)
    val reduceZipToSumRDD: RDD[(String, Int)] = filterZipToCountRDD.reduceByKey(_+_)

    // 4.5 将符合条件的日志数据根据页面ID进行分组聚合（B）
    val filterActionRDD: RDD[UserVisitAction] = usersRDD.filter(log => {
      targetIds.contains("" + log.page_id)
    })

    // (1, 100) => map(k, v)
    val pageActionRDD: RDD[(Long, Long)] = filterActionRDD.map(log => {
      (log.page_id, 1L)
    }).reduceByKey(_ + _)

    val pageActionMap: Map[Long, Long] = pageActionRDD.collect().toMap

    val taskId = UUID.randomUUID().toString

    reduceZipToSumRDD.foreach(println)
    println("==================")
    // 4.6 将计算结果A / B,获取转换率
    reduceZipToSumRDD.foreach{
      case (pageflow, sum) => {

        var pageid = pageflow.split("_")(0)

        var pageClickSumcount = pageActionMap(pageid.toLong)

        var rate = (sum.toDouble / pageClickSumcount * 100).toInt

        println((taskId, pageflow, rate ))
      }
    }

    // 4.7 将转换率通过JDBC保存到Mysql中


    // *********************************************************

    // 释放资源
    sparkSession.stop()*/

  }

}
