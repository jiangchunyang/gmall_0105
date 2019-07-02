package com.atguigu.gmall.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall.canal.util.MyKafkaSender;
import com.atguigu.gmall.constant.GmallConstants;

import java.util.List;
import java.util.Random;

public class CanalHandle {

    String tableName;
    CanalEntry.EventType eventType;
    List<CanalEntry.RowData> rowDatasList;

    public CanalHandle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        this.tableName = tableName;
        this.eventType = eventType;
        this.rowDatasList = rowDatasList;
    }


    //发送数据到不同的kafka的topic
    public void handle() {
        if (eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_info")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER);
        } else if ((eventType.equals(CanalEntry.EventType.INSERT) || eventType.equals(CanalEntry.EventType.UPDATE)) && tableName.equals("user_info")) {
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_USER);
        }else if("order_detail".equals(tableName) && eventType.equals(CanalEntry.EventType.INSERT)){
            sendRowList2Kafka(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }

    }


    //处理发送到kafka的数据
    public void sendRowList2Kafka(String kafkaTopic) {
        //一行的数据封装成一个JSONString，发送到kafka
        for (CanalEntry.RowData rowData : rowDatasList) {

            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "-->" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }

            try {
                Thread.sleep(new Random().nextInt(5)*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }
    }

}
