package com.work.sparkmall

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util

import com.work.sparkmall.common.util.ConfigurationUtil
import com.work.sparkmall.model.UserVisitAction
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}

import scala.collection.mutable

/**
  * @author star
  */
object TopTenActive {
  def main(args: Array[String]): Unit = {

//    Top10 热门品类中 Top10 活跃 Session 统计
    //连接数据库获取top10catagoryid

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("TopTenActive")
    val context = new SparkContext(sparkConf)

    val driver = ConfigurationUtil.getValueByKey("jdbc.driver.class")
    val url = ConfigurationUtil.getValueByKey("jdbc.url")
    val user = ConfigurationUtil.getValueByKey("jdbc.user")
    val password = ConfigurationUtil.getValueByKey("jdbc.password")

    val sql = "select taskId,category_id from sparkmall.category_top10 where 1 = 1 or (category_id >= ? and category_id <= ?)"
    val jdbcRDD: JdbcRDD[(String,String)] = new JdbcRDD(context, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, user, password)
    }, sql, 1, 10, 1, rs => {
      (rs.getString(2),rs.getString(1))
    })

    val jdbcMaps: Map[String, String] = jdbcRDD.collect().toMap

    val valuesJDBC: List[String] = jdbcMaps.keys.toList
    println("values:"+valuesJDBC)
//    val topCatagory: String = jdbcRDD.reduce((c1,c2)=>("",c1._2+","+c2._2))._2.toString

    //因为是在rdd里面执行，所以需要累加器
//    val map = new mutable.HashMap[String,String]()

    val taskidMap: CollectionAccumulator[(String, String)] = context.collectionAccumulator[(String,String)]("taskId")

    jdbcRDD.foreach{
      case (taskid,catagoryid)=>{
        taskidMap.add((catagoryid,taskid))
      }
    }
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //记得导入这个东西
    import spark.implicits._
    //找出所有top10品类对应的数据
    spark.sql("use " + ConfigurationUtil.getValueByKey("hive.database"))
    val userRDD: RDD[UserVisitAction] = spark.sql("select * from user_visit_action").as[UserVisitAction].rdd

    //过滤，结构转换，聚合,分组(catagoryid,((sessonid,count),(sessonid,count)))
    val filterRDD: RDD[UserVisitAction] = userRDD.filter(user => {
//      println(values.contains(user.click_category_id + ""))
      valuesJDBC.contains(user.click_category_id + "")
    })

    val mapRDD: RDD[(String, Int)] = filterRDD.map(user => (user.click_category_id + "_" + user.session_id, 1)).reduceByKey(_ + _)

    val groupbyresult: RDD[(String, Iterable[(String, Int)])] = mapRDD.map {
      case (csid, sum) => {
        val strings: Array[String] = csid.split("_")
        (strings(0), (strings(1), sum))
      }
    }.groupByKey()

    println("==================")
    //根据品类分组获取每个分组的top10
    val result: RDD[(String, List[(String, Int)])] = groupbyresult.mapValues(list => {
      list.toList.sortWith {
        case (left, right) => {
          left._2 > right._2
        }
      }.take(10)
    })

    println("===============")


    result.foreachPartition(lists=>{
      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, user, password)
      val sql = "insert into category_top10_session_count values(?,?,?,?)"
      val ps: PreparedStatement = connection.prepareStatement(sql)

      lists.foreach{
        case (catagoryid,list)=>{
          list.foreach{
            case (sessionid,sum)=>{
              ps.setObject(1,jdbcMaps.get(catagoryid).getOrElse("").toString)
              ps.setObject(2,catagoryid)
              ps.setObject(3,sessionid)
              ps.setObject(4,sum)
              ps.executeUpdate()
            }
          }
        }
      }
      ps.close()
      connection.close
    })
    context.stop()
  }
}
