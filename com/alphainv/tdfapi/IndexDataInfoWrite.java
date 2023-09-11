package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class IndexDataInfoWrite implements Runnable{
    protected Boolean quitFlag = false;
    BlockingQueue<IndexDataInfo> indexDataInfoQueue;
    ConcurrentHashMap<String,Double> indexSDMapAll;

    public IndexDataInfoWrite(BlockingQueue<IndexDataInfo> indexDataInfoQueue, ConcurrentHashMap<String, Double> indexSDMapAll) {
        this.indexDataInfoQueue = indexDataInfoQueue;
        this.indexSDMapAll = indexSDMapAll;
    }

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;

    }

    @Override
    public void run() {
        Long countNum = 0l;
//        Date statDate = new Date();
//        long statDateLong = statDate.getTime();
//        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
//        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
//        String ymdStr = ymdDF.format(statDate);
        while (!quitFlag){
            IndexDataInfo indexDataInfoInstance = indexDataInfoQueue.poll();
            if (countNum%200000==0){
                int queueSize = indexDataInfoQueue.size();
                System.out.println("指数管道已处理"+countNum+", 当前index队列长度还剩"+queueSize);
            }
//                if (countNum%2==0){
//                    int queueSize = marketDataInfoQueue.size();
//                    System.out.println("市场管道已处理"+countNum+", 当前market队列长度还剩"+queueSize);
//                }
            if (indexDataInfoInstance!=null){
                String indexCodeNum = indexDataInfoInstance.getIndexCode();
                Long nOpenIndex = indexDataInfoInstance.getnOpenIndex();
                Long nLastIndex = indexDataInfoInstance.getnLastIndex();
                Double indexSdRate = 0D;
                if (nOpenIndex!=null && nLastIndex!=null && !nOpenIndex.equals(0L)){
                    indexSdRate = ((double)nLastIndex-nOpenIndex)/nOpenIndex;
                }
                indexSDMapAll.put(indexCodeNum,indexSdRate);
                countNum++;

            }
        }

    }
}
