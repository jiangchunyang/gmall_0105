package com.atguigu.gmall0105.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.OrderInfo
import com.atguigu.gmall0105.realtime.util.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {

  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //从Kafka获取数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //需要补充日期字段
      val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)

      //电话脱敏:splitAt(7)按照电话的第7位进行切分
      val tuple: (String, String) = orderInfo.consignee_tel.splitAt(7)
      orderInfo.consignee_tel = "*" * 7 + tuple._2

      orderInfo

      //增加一个字段，标识是否是用户首次下单
    })


    //保存到Hbase + phoenix
    orderInfoDStream.foreachRDD(rdd=>{
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("gmall0105_order_info",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID",
          "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO",
          "PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,
        Some("hadoop102,hadoop103,hadoop104:2181") )
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
