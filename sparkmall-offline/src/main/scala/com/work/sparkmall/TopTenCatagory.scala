package com.work.sparkmall

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.work.sparkmall.common.util.ConfigurationUtil
import com.work.sparkmall.model.{CategoryTop10, UserVisitAction}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @author star
  */
object TopTenCatagory {

  def main(args: Array[String]): Unit = {
    //    获取点击、下单和支付数量排名前 10 的品类

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TopTenCatagory")

    //获取sparksession
    //1，获取hive中的时间表数据,此处必须要使用支持hive
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    spark.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))

    val builder = new StringBuilder("select * from user_visit_action where 1 = 1")
    //这里可以对数据进行过滤
    /*val startDate = ConfigurationUtil.getCondValue("startDate")
    val endDate = ConfigurationUtil.getCondValue("endDate")

    if (startDate != null && !"".equals(startDate.trim)){
      builder.append(" and action_time >= '").append(startDate).append("'")
    }
    if (endDate != null && !"".equals(endDate.trim)){
      builder.append(" and action_time <= '").append(endDate).append("'")
    }*/


    val hiveDF: DataFrame = spark.sql(builder.toString)

    //转换为RDD:结构：(user)
    val actionRDD: RDD[UserVisitAction] = hiveDF.as[UserVisitAction].rdd

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
    //希望得到的数据结构：（catagoryid,clicksum,ordersum,paysum）
    //自定义累加器完成对数据的汇总转换
    val sumAcclumate = new CalcuteSumAcclumate
    spark.sparkContext.register(sumAcclumate)
    actionRDD.foreach(user => {
      if (user.click_category_id != -1) {
        sumAcclumate.add(user.click_category_id + "_click")
      } else if (user.order_category_ids != null) {
        val ids: Array[String] = user.order_category_ids.split(",")
        ids.foreach(id => sumAcclumate.add(id + "_order"))
      } else if (user.pay_category_ids != null) {
        val ids: Array[String] = user.pay_category_ids.split(",")
        ids.foreach(id => sumAcclumate.add(id + "_pay"))
      }
    })

    //获取累加器数据 结果：（id_click,count）
    val result: mutable.HashMap[String, Long] = sumAcclumate.value

    //将聚合的数据融合在一起（category, (sumClick, sumOrder, sumPay)）

    //    取点击、下单和支付数量排名前 10 的品类
    val statMap: Map[String, mutable.HashMap[String, Long]] = result.groupBy {
      case (catagory, sum) => {
        catagory.split("_")(0)
      }
    }
    statMap.take(10).foreach(println)

    val taskID: String = UUID.randomUUID().toString
    val list: List[CategoryTop10] = statMap.map {
      case (categoryid, map) => {
        CategoryTop10(taskID, categoryid,
          map.getOrElse(categoryid+"_click", 0l),
          map.getOrElse(categoryid+"_order", 0l),
          map.getOrElse(categoryid+"_pay", 0l))
      }
    }.toList
    //（品类，（点击，下单，支付））
    //获取结果：（品类，点击数，下单数，支付数）
    val top1s: List[CategoryTop10] = list.sortWith((c1, c2) => {
      if (c1.clickCount > c2.clickCount) {
        true
      } else if (c1.clickCount < c2.clickCount) {
        false
      } else {
        if (c1.orderCount > c2.orderCount) {
          true
        } else if (c1.orderCount < c2.orderCount) {
          false
        } else {
          c1.payCount > c2.payCount
        }
      }
    }).take(10)


    val driver = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigurationUtil.getValueByKey("jdbc.url")
    val user = ConfigurationUtil.getValueByKey("jdbc.user")
    val password = ConfigurationUtil.getValueByKey("jdbc.password")

    Class.forName(driver)

    val connection: Connection = DriverManager.getConnection(url,user,password)

    val sql = "insert into category_top10 values(?,?,?,?,?)"

    val ps: PreparedStatement = connection.prepareStatement(sql)

    top1s.foreach(data=>{
      ps.setObject(1,data.taskId)
      ps.setObject(2,data.categoryId)
      ps.setObject(3,data.clickCount)
      ps.setObject(4,data.orderCount)
      ps.setObject(5,data.payCount)

      ps.executeUpdate()
    })

    ps.close()
    connection.close()
    spark.stop()
  }
}

//通过累加器返回数据为(map(catagorytype,sum))
class CalcuteSumAcclumate extends AccumulatorV2[String, mutable.HashMap[String, Long]] {
  private var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = new CalcuteSumAcclumate

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    map(v) = map.getOrElse(v, 0l) + 1l
  }

  //合并多个累加器的结果
  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    map = map.foldLeft(other.value){
      case (nmap, (k, v)) => {
        nmap(k) = nmap.getOrElse(k, 0l) + v
        nmap
      }
    }
  }

  override def value: mutable.HashMap[String, Long] = map
}