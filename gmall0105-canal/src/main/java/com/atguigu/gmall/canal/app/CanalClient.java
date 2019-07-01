package com.atguigu.gmall.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sun.rowset.internal.Row;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void watch(String hostname,int port,String destination,String tables){
        //建立连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination, "", "");

        while (true){
            canalConnector.connect();
            canalConnector.subscribe(tables);
            Message message = canalConnector.get(100);

            int size = message.getEntries().size();

            if (size ==0){

                System.out.println("没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {

                for (CanalEntry.Entry entry : message.getEntries()) {

                    if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA){

                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取操作的类型 insert update delete
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //获取行集 数据
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        CanalHandle canalHandle = new CanalHandle(tableName, eventType, rowDatasList);
                        canalHandle.handle();

                    }
                }





            }
        }
    }
}
