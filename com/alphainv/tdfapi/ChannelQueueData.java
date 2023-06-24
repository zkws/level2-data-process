package com.alphainv.tdfapi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ChannelQueueData {

    private String channelCode;

    private Long[] orderPriceArray = new Long[300000000];

    private BlockingQueue<Object[]> orderBuyQueue = new LinkedBlockingQueue<Object[]>();

    private BlockingQueue<Object[]> orderSellQueue = new LinkedBlockingQueue<Object[]>();

    private DelayQueue<DelayedOrder> m5OrderBuyDelayQueue = new DelayQueue<DelayedOrder>();

    private DelayQueue<DelayedOrder> m5OrderSellDelayQueue = new DelayQueue<DelayedOrder>();

    private BlockingQueue<Object[]> transBuyQueue = new LinkedBlockingQueue<Object[]>();

    private BlockingQueue<Object[]> transSellQueue = new LinkedBlockingQueue<Object[]>();

    private ConcurrentHashMap<String,Long> newestPriceMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Long> highLimitPriceMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Long> lowLimitPriceMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Integer> highLimitFlagMap = new ConcurrentHashMap<String,Integer>();

    private ConcurrentHashMap<String,Integer> lowLimitFlagMap = new ConcurrentHashMap<String,Integer>();

    private ConcurrentHashMap<String,String> stkSDMap = new ConcurrentHashMap<String,String>();;

    private ConcurrentHashMap<String,Long> orderBuyMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Long> orderSellMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Double> orderBuyWeightedMap = new ConcurrentHashMap<String,Double>();

    private ConcurrentHashMap<String,Double> orderSellWeightedMap = new ConcurrentHashMap<String,Double>();

    private ConcurrentHashMap<String,Long> order5MBuyMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Long> order5MSellMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Long> transBuyMap = new ConcurrentHashMap<String,Long>();

    private ConcurrentHashMap<String,Long> transSellMap = new ConcurrentHashMap<String,Long>();

    public Long[] getOrderPriceArray() {
        return orderPriceArray;
    }

    public BlockingQueue<Object[]> getOrderBuyQueue() {
        return orderBuyQueue;
    }

    public BlockingQueue<Object[]> getOrderSellQueue() {
        return orderSellQueue;
    }

    public BlockingQueue<Object[]> getTransBuyQueue() {
        return transBuyQueue;
    }

    public BlockingQueue<Object[]> getTransSellQueue() {
        return transSellQueue;
    }

    public ConcurrentHashMap<String, Double> getOrderBuyWeightedMap() {
        return orderBuyWeightedMap;
    }

    public ConcurrentHashMap<String, Double> getOrderSellWeightedMap() {
        return orderSellWeightedMap;
    }

    public String getChannelCode() {
        return channelCode;
    }

    public DelayQueue<DelayedOrder> getM5OrderBuyDelayQueue() {
        return m5OrderBuyDelayQueue;
    }

    public DelayQueue<DelayedOrder> getM5OrderSellDelayQueue() {
        return m5OrderSellDelayQueue;
    }

    public ConcurrentHashMap<String, Long> getNewestPriceMap() {
        return newestPriceMap;
    }

    public ConcurrentHashMap<String, Integer> getHighLimitFlagMap() {
        return highLimitFlagMap;
    }

    public ConcurrentHashMap<String, Integer> getLowLimitFlagMap() {
        return lowLimitFlagMap;
    }

    public ConcurrentHashMap<String, String> getStkSDMap() {
        return stkSDMap;
    }

    public ConcurrentHashMap<String, Long> getOrderBuyMap() {
        return orderBuyMap;
    }

    public ConcurrentHashMap<String, Long> getOrderSellMap() {
        return orderSellMap;
    }

    public ConcurrentHashMap<String, Long> getOrder5MBuyMap() {
        return order5MBuyMap;
    }

    public ConcurrentHashMap<String, Long> getOrder5MSellMap() {
        return order5MSellMap;
    }

    public ConcurrentHashMap<String, Long> getTransBuyMap() {
        return transBuyMap;
    }

    public ConcurrentHashMap<String, Long> getTransSellMap() {
        return transSellMap;
    }

    public ConcurrentHashMap<String, Long> getHighLimitPriceMap() {
        return highLimitPriceMap;
    }

    public ConcurrentHashMap<String, Long> getLowLimitPriceMap() {
        return lowLimitPriceMap;
    }

    ChannelQueueData(String channelCode){
        this.channelCode=channelCode;
    }
}
