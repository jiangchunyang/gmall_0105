package com.atguigu.gmall0105.publisher.service;

import java.util.Map;

public interface PublisherService {


    public Long getDauTotal(String date);

    public Map getDauHour(String date);

    Double getOrderAmount(String date);

    Map getOrderAmountHour(String date);
}
