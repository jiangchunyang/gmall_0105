package com.atguigu.gmall0105.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.constant.GmallConstants
import com.atguigu.gmall0105.realtime.bean.StartUpLog
import com.atguigu.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._


//日活
object DAUApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DAUApp")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //1.数据流转换,将数据转换成样例类的格式
    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.map { record =>
      val jsonStr: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
      val dateArr: Array[String] = sdf.format(new Date(startUpLog.ts)).split(" ")
      startUpLog.logDate = dateArr(0)
      startUpLog.logHour = dateArr(1)
      startUpLog
    }


    //需要行动算子触发
    startUpLogDStream.cache()



    //2.对数据进行过滤，去重，只保留清单中不存在的数据（批次之间）
    val filterDStream: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      val client: Jedis = RedisUtil.getJedisClient
      val time: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

      val key = "dau:" + time
      val dauSet: util.Set[String] = client.smembers(key)
      client.close()

      val userBroadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)
      println("过滤前：" + rdd.count())

      val filterRDD: RDD[StartUpLog] = rdd.filter { startuplog => {
        !userBroadcast.value.contains(startuplog.mid)
      }}

      println("过滤后：" + filterRDD.count())
      filterRDD
    })


    //3.对批次之内的数据进行去重(对mid进行分组，取每个组内第一个)
    val groupDStream: DStream[(String, Iterable[StartUpLog])] = filterDStream.map(log=>(log.mid,log)).groupByKey()
    val distinctDStream: DStream[StartUpLog] = groupDStream.flatMap {
      case (mid, datas) => {
        datas.toList.take(1)
      }
    }


    //4.将结果保存在redis中 key：string   v：（k，v） set
    //key:  dau:2019-xx-xx    value: mid
    distinctDStream.foreachRDD(rdd=>{

      rdd.foreachPartition(datas=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient
        datas.foreach{log=> {
          val key = "dau:" + log.logDate
          jedisClient.sadd(key,log.mid)
        }}
        jedisClient.close()
      })
    })


    //把数据写入hbase+phoenix
    distinctDStream.foreachRDD{
      rdd=>{
        rdd.saveToPhoenix("GMALL0105_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,
          new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
