package com.atguigu.gmall.canal.app;

public class CanalMain {
    public static void main(String[] args) {
        CanalClient.watch("hadoop102",11111,"example1","gmall0105.order_info");
    }
}
