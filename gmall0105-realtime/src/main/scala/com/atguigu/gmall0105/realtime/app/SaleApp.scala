package com.atguigu.gmall0105.realtime.app

import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaleApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER,ssc)
  }
}
