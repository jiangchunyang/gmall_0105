package com.atguigu.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.{CouponAlertInfo, EventInfo}
import com.atguigu.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks.{break, breakable}

object EventApp {

  def main(args: Array[String]): Unit = {

    //创建环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EventApp")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // TODO 1.从kafka获取消息后，转换成样例类
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    val eventInfoDStream: DStream[EventInfo] = kafkaDStream.map(record => {
      val jsonStr: String = record.value()
      val eventInfo: EventInfo = JSON.parseObject(jsonStr, classOf[EventInfo])
      eventInfo
    })

    // TODO 2.开窗口
    val windowDStream: DStream[EventInfo] = eventInfoDStream.window(Seconds(30), Seconds(5))


    // TODO 3.对数据的mid进行分组
    val groupMidDStream: DStream[(String, Iterable[EventInfo])] = windowDStream.map(event => (event.mid, event)).groupByKey()

//        groupMidDStream.print()

    // TODo 4.进行预警判断
    //    在一个设备之内
    //    1 三次及以上的领取优惠券 (evid coupon) 且 uid都不相同
    //    2 没有浏览商品(evid  clickItem)
    val alertDStream: DStream[(Boolean, CouponAlertInfo)] = groupMidDStream.map { case (mid, eventInfoItr) =>
      val couponUidsSet = new util.HashSet[String]()
      val itemIdsSet = new util.HashSet[String]()
      val eventIds = new util.ArrayList[String]()
      var notClickItem: Boolean = true
      breakable(
        for (eventInfo: EventInfo <- eventInfoItr) {
          eventIds.add(eventInfo.evid) //用户行为
          if (eventInfo.evid == "coupon") {
            couponUidsSet.add(eventInfo.uid) //用户领券的uid
            itemIdsSet.add(eventInfo.itemid) //用户领券的商品id
          } else if (eventInfo.evid == "clickItem") {
            notClickItem = false
            break()
          }
        }
      )
      //组合成元祖  （标识是否达到预警要求，预警信息对象）
      (couponUidsSet.size() >= 3 && notClickItem, CouponAlertInfo(mid, couponUidsSet, itemIdsSet, eventIds, System.currentTimeMillis()))
    }

//        alertDStream.print()

    // TODO 5.对预警后的数据第一位为true的数据过滤(留下预警的信息)
    val filteredDStream: DStream[(Boolean, CouponAlertInfo)] = alertDStream.filter(_._1)

//    filteredDStream.print()

    // TODO 6.将数据增加一个id，用于保存到es中去重
    val infoWithIdDStream: DStream[(String, CouponAlertInfo)] = filteredDStream.map {
      case (flag, info) => {
        val id: String = info.mid + "_" + info.ts / 1000L / 60L

        (id, info)
      }
    }

    // TODO 7.将数据保存到es中
    infoWithIdDStream.foreachRDD(rdd => {
      rdd.foreachPartition { datas => {
        MyEsUtil.insertBulk(GmallConstants.ES_INDEX_COUPON_ALERT, datas.toList)
      }
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
