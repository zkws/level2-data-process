package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class StockStat {

    public String getBuySaleTransStat(Jedis realJedisInstance,String ymdStr, String listKeyTime, ArrayList<String> subStocks){
        for (String singleStockCode:subStocks) {
            String singleStockCodeNum = singleStockCode.substring(0,6);
            String listKeyBuy = listKeyTime + "-" +singleStockCodeNum+"-66";
            String listKeySell = listKeyTime + "-" +singleStockCodeNum+"-83";
            String accuKeyBuy = ymdStr + "-" +singleStockCodeNum+"-66-a";
            String accuKeySell = ymdStr + "-" +singleStockCodeNum+"-83-a";
            List<String> single_time_buy_list = realJedisInstance.lrange(listKeyBuy,0,realJedisInstance.llen(listKeyBuy));
            List<String> single_time_sell_list = realJedisInstance.lrange(listKeySell,0,realJedisInstance.llen(listKeySell));
            long single_time_buy_total = single_time_buy_list.stream().mapToLong(i->Long.valueOf(i)).sum();
            long single_time_sell_total = single_time_sell_list.stream().mapToLong(i->Long.valueOf(i)).sum();
            String accumListBuyStockObj =  realJedisInstance.get(accuKeyBuy);
            long accumListBuyStockNum = accumListBuyStockObj != null?Long.valueOf(accumListBuyStockObj):0;
            String accumListSellStockObj =  realJedisInstance.get(accuKeySell);
            long accumListSellStockNum = accumListSellStockObj != null?Long.valueOf(accumListSellStockObj):0;
            long accumListBuyStockNumNew = accumListBuyStockNum+single_time_buy_total;
            long accumListSellStockNumNew = accumListSellStockNum+single_time_sell_total;
            realJedisInstance.set(accuKeyBuy,String.valueOf(accumListBuyStockNumNew));
            realJedisInstance.set(accuKeySell,String.valueOf(accumListSellStockNumNew));
//          计算成交主买主卖比
//          TBSRATE: Transaction buy sale rate
            String transBsRate = "0";
            String transBsRateKey =  ymdStr + "-" +singleStockCodeNum+"-TBSRATE";
            if (Math.abs(accumListSellStockNumNew)>0){
                transBsRate = String.format("%.4f",(double)accumListBuyStockNumNew/accumListSellStockNumNew);
            }
            realJedisInstance.set(transBsRateKey,transBsRate);
        }
        return "stat done";
    }

    public String getSDStat(Jedis realJedisInstance, String ymdStr, String sdKeyTime, ArrayList<String> subStocks){
        for (String singleStockCode:subStocks) {
            String singleStockCodeNum = singleStockCode.substring(0,6);
            String sdKey = sdKeyTime + "-" +singleStockCodeNum+"-SD";
            String sdString = realJedisInstance.get(sdKey);
            String sdRateStr = "暂无数据";
            Double sdRate = 0D;
            String sdRateKey =  ymdStr + "-" +singleStockCodeNum+"-SDRate";
            if(sdString != null&&sdString.length()>0){
                String[] sdStringList = sdString.split(" ");
                Double currentPrice = Double.valueOf(sdStringList[0]);
                Double preClosePrice = Double.valueOf(sdStringList[1]);
                if (preClosePrice > 0 && currentPrice>0){
                    sdRate = (currentPrice-preClosePrice)/preClosePrice;
                    sdRateStr = String.format("%.4f",sdRate*100);
                    realJedisInstance.set(sdRateKey,sdRateStr);
//                    System.out.println("Write:"+sdRateKey+" "+sdRateStr);
                }
            }
        }
        return "stat done";
    }

    public String getBuySaleOrderStat(Jedis realJedisInstance, String ymdStr, String listKeyTime, String last5mListKeyTime, ArrayList<String> subStocks){
        Long highLowLimitOrderNum = 10000L;
        for (String singleStockCode:subStocks) {
            String singleStockCodeNum = singleStockCode.substring(0,6);

            String listKeyBuy = listKeyTime + "-" +singleStockCodeNum+"-BO";
            String listKeySell = listKeyTime + "-" +singleStockCodeNum+"-SO";
            String accuKeyBuy = ymdStr + "-" +singleStockCodeNum+"-BO-a";
            String accuKeySell = ymdStr + "-" +singleStockCodeNum+"-SO-a";

            // 5分钟压托比更新逻辑
            String last5mListKeyBuy = last5mListKeyTime + "-" +singleStockCodeNum+"-BO";
            String last5mListKeySell = last5mListKeyTime + "-" +singleStockCodeNum+"-SO";

            String accuKey5mBuy = ymdStr + "-" +singleStockCodeNum+"-BO-5ma";
            String accuKey5mSell = ymdStr + "-" +singleStockCodeNum+"-SO-5ma";


            String highLimitFlag = ymdStr+"-"+singleStockCodeNum+"-HL";
            String lowLimitFlag = ymdStr+"-"+singleStockCodeNum+"-LL";

//          HLP: high limit price
            String highLimitPriceKey = ymdStr+"-"+singleStockCodeNum+"-HLP";
//          LLP: low limit price
            String lowLimitPriceKey = ymdStr+"-"+singleStockCodeNum+"-LLP";

            String highLimitFlagValue =  realJedisInstance.get(highLimitFlag);
            String lowLimitFlagValue =  realJedisInstance.get(lowLimitFlag);

            long accumListBuyStockNumNew;
            long accumListSellStockNumNew;

            long accum5mListBuyStockNumNew;
            long accum5mListSellStockNumNew;

            List<String> single_time_buy_list = realJedisInstance.lrange(listKeyBuy,0,realJedisInstance.llen(listKeyBuy));
            long single_time_buy_total = single_time_buy_list.stream().mapToInt(i->Integer.valueOf(i)).sum();
            String accumListBuyStockObj =  realJedisInstance.get(accuKeyBuy);
            long accumListBuyStockNum = accumListBuyStockObj != null?Long.valueOf(accumListBuyStockObj):0;
            accumListBuyStockNumNew = accumListBuyStockNum+single_time_buy_total;

            List<String> single_time_buy_5m_list = realJedisInstance.lrange(last5mListKeyBuy,0,realJedisInstance.llen(last5mListKeyBuy));
            long single_time_buy_5m_total = single_time_buy_5m_list.stream().mapToInt(i->Integer.valueOf(i)).sum();
            String accum5mListBuyStockObj =  realJedisInstance.get(accuKey5mBuy);
            long accum5mListBuyStockNum = accum5mListBuyStockObj != null?Long.valueOf(accum5mListBuyStockObj):0;
            accum5mListBuyStockNumNew = accum5mListBuyStockNum+single_time_buy_total-single_time_buy_5m_total;

            if (lowLimitFlagValue!=null&&lowLimitFlagValue.equals("1")){
                String lowLimitPriceObj =  realJedisInstance.get(lowLimitPriceKey);
                long lowLimitPrice = lowLimitPriceObj != null?Long.valueOf(lowLimitPriceObj):0;
                //long lowLimitTurnover = lowLimitPrice*10000股/10000,
                // 除以第二个10000是因为交易所数据所给价格为原价*10000
                long lowLimitTurnover = lowLimitPrice*highLowLimitOrderNum/10000;
                if (accumListBuyStockNumNew<lowLimitTurnover){
                    accumListBuyStockNumNew=lowLimitTurnover;
                }
                if (accum5mListBuyStockNumNew<lowLimitTurnover){
                    accum5mListBuyStockNumNew = lowLimitTurnover;
                }
            }

            List<String> single_time_sell_list = realJedisInstance.lrange(listKeySell,0,realJedisInstance.llen(listKeySell));
            long single_time_sell_total = single_time_sell_list.stream().mapToInt(i->Integer.valueOf(i)).sum();
            String accumListSellStockObj =  realJedisInstance.get(accuKeySell);
            long accumListSellStockNum = accumListSellStockObj != null?Long.valueOf(accumListSellStockObj):0;
            accumListSellStockNumNew = accumListSellStockNum+single_time_sell_total;


            List<String> single_time_sell_5m_list = realJedisInstance.lrange(last5mListKeySell,0,realJedisInstance.llen(last5mListKeySell));
            long single_time_sell_5m_total = single_time_sell_5m_list.stream().mapToInt(i->Integer.valueOf(i)).sum();
            String accum5mListSellStockObj =  realJedisInstance.get(accuKey5mSell);
            long accum5mListSellStockNum = accum5mListSellStockObj != null?Long.valueOf(accum5mListSellStockObj):0;
            accum5mListSellStockNumNew = accum5mListSellStockNum+single_time_sell_total-single_time_sell_5m_total;


            if (highLimitFlagValue!=null&&highLimitFlagValue.equals("1")){
                String highLimitPriceObj =  realJedisInstance.get(highLimitPriceKey);
                long highLimitPrice = highLimitPriceObj != null?Long.valueOf(highLimitPriceObj):0;
                long highLimitTurnover = highLimitPrice*highLowLimitOrderNum/10000;

                if (accumListSellStockNumNew<highLimitTurnover){
                    accumListSellStockNumNew=highLimitTurnover;
                }
                if (accum5mListSellStockNumNew<highLimitTurnover){
                    accum5mListSellStockNumNew = highLimitTurnover;
                }
            }


            realJedisInstance.set(accuKeyBuy,String.valueOf(accumListBuyStockNumNew));

            realJedisInstance.set(accuKeySell,String.valueOf(accumListSellStockNumNew));

            realJedisInstance.set(accuKey5mBuy,String.valueOf(accum5mListBuyStockNumNew));

            realJedisInstance.set(accuKey5mSell,String.valueOf(accum5mListSellStockNumNew));

//          计算压托比
//          OBSRATE: order buy sale rate
//          OBSRATES: order buy sale rate source
            String orderBSRate = "0";
            String orderBSRateSource="";
            String orderBSRateKey = ymdStr + "-" +singleStockCodeNum+"-OBSRATE";
            String orderBSRateSourceKey = ymdStr + "-" +singleStockCodeNum+"-OBSRATES";
            if (accumListBuyStockNumNew>0){
                orderBSRateSource+="买正";
            }
            else if (accumListBuyStockNumNew<0){
                orderBSRateSource+="买负";
            }
            else{
                orderBSRateSource+="买0";
            }

            if (accumListSellStockNumNew>0){
                orderBSRateSource+="卖正";
                orderBSRate = String.format("%.4f", (double) accumListBuyStockNumNew /accumListSellStockNumNew);
            }
            else if (accumListSellStockNumNew<0){
                orderBSRateSource+="卖负";
                orderBSRate = String.format("%.4f",(double)accumListBuyStockNumNew/accumListSellStockNumNew);
            }
            else{
                orderBSRateSource+="卖0";
            }
            realJedisInstance.set(orderBSRateKey,orderBSRate);
            realJedisInstance.set(orderBSRateSourceKey,orderBSRateSource);

//          计算5分钟压托比
            String orderBSRateM5 = "0";
            String orderBSRateM5Source="";
            String orderBSRateM5Key = ymdStr + "-" +singleStockCodeNum+"-OBSRATEM5";
            String orderBSRateM5SourceKey = ymdStr + "-" +singleStockCodeNum+"-OBSRATEM5S";
            if (accum5mListBuyStockNumNew>0){
                orderBSRateM5Source+="买正";
            }
            else if (accum5mListBuyStockNumNew<0){
                orderBSRateM5Source+="买负";
            }
            else{
                orderBSRateM5Source+="买0";
            }

            if (accum5mListSellStockNumNew>0){
                orderBSRateM5Source+="卖正";
                orderBSRateM5 = String.format("%.4f", (double) accum5mListBuyStockNumNew /accum5mListSellStockNumNew);
            }
            else if (accum5mListSellStockNumNew<0){
                orderBSRateM5Source+="卖负";
                orderBSRateM5 = String.format("%.4f",(double)accum5mListBuyStockNumNew/accum5mListSellStockNumNew);
            }
            else{
                orderBSRateM5Source+="卖0";
            }
            realJedisInstance.set(orderBSRateM5Key,orderBSRateM5);
            realJedisInstance.set(orderBSRateM5SourceKey,orderBSRateM5Source);

        }
        return "stat done";
    }

    public void getStockStat(long statDateLong, ArrayList<String> portfolioStocks, JedisPool[] jedisPoolArray, Integer redisIndex) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        Date statDate = new Date();
        statDate.setTime(statDateLong);
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
        String ymdStr = ymdDF.format(statDate);
        String statDateStr = fullDF.format(statDate);
        statDateStr = statDateStr.substring(0,statDateStr.length()-1);
//        收盘时间
//        Date closeTime = fullDF.parse(dateStr+" 150000");
//      回放模式测试时间，主要设置为回放模式不再可用的时间。
        Date last5mStatTime = new Date();
        Jedis realJedisInstance = null;
        if(redisIndex!=null){
            try {
                // 从连接池获取jedis对象
                realJedisInstance = jedisPoolArray[redisIndex].getResource();
                long startLong = new Date().getTime();
                last5mStatTime.setTime(statDateLong-300000);
                String fullLast5mTimeStr = fullDF.format(last5mStatTime);
                fullLast5mTimeStr = fullLast5mTimeStr.substring(0,fullLast5mTimeStr.length()-1);
                getSDStat(realJedisInstance, ymdStr, statDateStr, portfolioStocks);
                getBuySaleOrderStat(realJedisInstance,ymdStr, statDateStr, fullLast5mTimeStr, portfolioStocks);
                getBuySaleTransStat(realJedisInstance, ymdStr, statDateStr, portfolioStocks);
                Date finishTime = new Date();
                long finishLong = finishTime.getTime();
                int threadNum = Thread.currentThread().getThreadGroup().activeCount();
                System.out.println(Thread.currentThread().getName()+"已在 "+fullDF.format(finishTime)+" 完成更新，统计时间："+statDateStr+" 组合内数量"+portfolioStocks.size()+" 耗时："+(finishLong-startLong)+"毫秒, 当前线程池线程数："+threadNum);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (null != realJedisInstance) {
                    realJedisInstance.close();
                }
            }
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, ParseException {

    }
}
