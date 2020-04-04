package com.atguigu.sparkmall.model

/**
  * @author star 
  */
class DataModel {

}

/**
  *用户行为
  * @param date   用户点击行为的日期
  * @param user_id  用户id
  * @param session_id session id
  * @param page_id  页面id
  * @param action_time  点击行为时间点
  * @param search_keyword  搜索关键字
  * @param click_category_id 商品品类id
  * @param click_product_id 某个商品id
  * @param order_category_ids 下单品类ids
  * @param order_product_ids  一次订单中所有品类的id集合
  * @param pay_category_ids 一次支付的素有品类的id集合
  * @param pay_product_ids  一次支付素有商品的id集合
  * @param city_id  城市id
  */
case class UserVisitAction(date:String,user_id:Long,session_id:String,page_id:Long,action_time:String,search_keyword:String,
                           click_category_id:Long,click_product_id:Long,order_category_ids:String,order_product_ids:String,pay_category_ids:String,
                           pay_product_ids:String,city_id:Long)


/**
  *用户信息
  * @param user_id  用户id
  * @param username 用户名称
  * @param name 名字
  * @param age  年龄
  * @param professional 职业
  * @param gender 性别
  */
case class UserInfo(user_id:Long,username:String,name:String,age:Int,professional:String,gender:String)

/**
  * 产品信息
  * @param product_id 商品id
  * @param product_name 上坪名称
  * @param extend_info  额外信息
  */
case class ProductInfo(product_id:Long,product_name:String,extend_info:String)

/**
  * 城市信息
  * @param city_id 城市id
  * @param city_name  城市名称
  * @param area 取域
  */
case class CityInfo(city_id:Long,city_name:String,area:String)

case class CategoryTop10( taskId : String, categoryId : String, clickCount : Long, orderCount : Long, payCount : Long )

case class CategoryTop10SessionId( taskId : String, categoryId : String, sessionId : String, clickCount : Long)

//时间          区域  cityid  userid  adid
case class AdClick(time:String,area:String,cid:String,uid:String,adid:String)