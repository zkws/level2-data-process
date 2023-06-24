package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class MarketDataInfoWrite implements Runnable{
    protected Boolean quitFlag = false;
    BlockingQueue<MarketDataInfo> marketDataInfoQueue;
    ChannelQueueData[] channelQueueDataArray;
    HashMap<String,Integer> channelCodeMap;
    HashMap<String,String> stkChannelMap;
    JedisPool jedisPool;

    MarketDataInfoWrite(BlockingQueue<MarketDataInfo> marketDataInfoQueue,ChannelQueueData[] channelQueueDataArray,
                        HashMap<String,Integer> channelCodeMap,HashMap<String,String> stkChannelMap,
                        JedisPool jedisPool
    ){
        this.marketDataInfoQueue = marketDataInfoQueue;
        this.channelQueueDataArray = channelQueueDataArray;
        this.channelCodeMap = channelCodeMap;
        this.stkChannelMap = stkChannelMap;
        this.jedisPool = jedisPool;
    }

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;

    }

    @Override
    public void run() {
        Long countNum = 0l;
        Date statDate = new Date();
//        long statDateLong = statDate.getTime();
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
        String ymdStr = ymdDF.format(statDate);
        Jedis realJedisInstance = null;
        try {
            realJedisInstance = jedisPool.getResource();
            while (!quitFlag){
                MarketDataInfo marketDataInfoInstance = marketDataInfoQueue.poll();
                if (countNum%200000==0){
                    int queueSize = marketDataInfoQueue.size();
                    System.out.println("市场管道已处理"+countNum+", 当前market队列长度还剩"+queueSize);
                }
//                if (countNum%2==0){
//                    int queueSize = marketDataInfoQueue.size();
//                    System.out.println("市场管道已处理"+countNum+", 当前market队列长度还剩"+queueSize);
//                }
                if (marketDataInfoInstance!=null){
                    String singleStockCodeNum = marketDataInfoInstance.getStockCode();
                    String highLimitKey = ymdStr+"-"+singleStockCodeNum+"-HFLAG";
                    String lowLimitKey = ymdStr+"-"+singleStockCodeNum+"-LFLAG";
                    Long highLimit = marketDataInfoInstance.getHighLimit();
                    Long lowLimit = marketDataInfoInstance.getLowLimit();
                    Long recentPrice = marketDataInfoInstance.getRecentPrice();
                    Long preClosePrice = marketDataInfoInstance.getPreClose();
                    String stkChannelCode = stkChannelMap.get(singleStockCodeNum);
                    if (stkChannelCode!=null){
                        int channelIndex = channelCodeMap.get(stkChannelCode);
                        ChannelQueueData channelQueueData = channelQueueDataArray[channelIndex];
                        ConcurrentHashMap<String, Long> newestPriceMap=channelQueueData.getNewestPriceMap();
                        ConcurrentHashMap<String,Long> highLimitPriceMap = channelQueueData.getHighLimitPriceMap();
                        ConcurrentHashMap<String,Long> lowLimitPriceMap = channelQueueData.getLowLimitPriceMap();
                        ConcurrentHashMap<String, Integer> highLimitFlagMap=channelQueueData.getHighLimitFlagMap();
                        ConcurrentHashMap<String, Integer> lowLimitFlagMap=channelQueueData.getLowLimitFlagMap();
                        ConcurrentHashMap<String, String> stkSDMap=channelQueueData.getStkSDMap();
                        newestPriceMap.put(singleStockCodeNum,recentPrice);
                        highLimitPriceMap.put(singleStockCodeNum,highLimit);
                        lowLimitPriceMap.put(singleStockCodeNum,lowLimit);
                        if (preClosePrice > 0 && recentPrice>0){
                            Double sdRate = ((double)recentPrice-preClosePrice)/preClosePrice;
                            String sdRateStr = String.format("%.1f",sdRate*100);
//                            System.out.println("当日涨跌"+recentPrice+" "+preClosePrice+" "+sdRateStr);
                            stkSDMap.put(singleStockCodeNum,sdRateStr);
                        }
                        if (recentPrice.equals(highLimit)||recentPrice>highLimit){
                            highLimitFlagMap.put(singleStockCodeNum, 1);
                            realJedisInstance.set(highLimitKey,"1");
                        }
                        else {
                            highLimitFlagMap.put(singleStockCodeNum, 0);
                            realJedisInstance.set(highLimitKey,"0");
                        }
                        if (recentPrice.equals(lowLimit)||recentPrice<highLimit){
                            lowLimitFlagMap.put(singleStockCodeNum, 1);
                            realJedisInstance.set(lowLimitKey,"1");
                        }
                        else {
                            lowLimitFlagMap.put(singleStockCodeNum, 0);
                            realJedisInstance.set(lowLimitKey,"0");
                        }
                        countNum++;
                    }
                    else{
                        marketDataInfoQueue.offer(marketDataInfoInstance);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != realJedisInstance) {
                realJedisInstance.close();
            }
        }

    }
}
