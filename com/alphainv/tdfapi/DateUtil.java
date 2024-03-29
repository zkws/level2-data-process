package com.alphainv.tdfapi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {


//overTimeUnit 收盘后再继续工作多少个间隔的时间单位
    public static ArrayList<Long> getdelayTimeList(Long statDateLong, Long intervalMilliSecond, int overTimeUnit) throws ParseException {
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
//        System.out.println(fullDF.format(new Date()));
//        statDate = fullDF.parse("20230517-092000");
//        Long statDateLong = statDate.getTime();
        Date statDate = new Date();
        statDate.setTime(statDateLong);
//        statDateLong = fullDF.parse("20230522-092000").getTime();
        String ymdStr = ymdDF.format(statDate);
        String amOpenTimeStr = ymdStr+"-091500";
        String amCloseTimeStr = ymdStr+"-113000";
        String pmOpenTimeStr = ymdStr+"-130000";
        String pmCloseTimeStr = ymdStr+"-150000";
        Date amOpenTime = fullDF.parse(amOpenTimeStr);
        Date amCloseTime = fullDF.parse(amCloseTimeStr);
        Date pmOpenTime = fullDF.parse(pmOpenTimeStr);
        Date pmCloseTime = fullDF.parse(pmCloseTimeStr);
        Long amOpenTimeLong = amOpenTime.getTime();
        Long amCloseTimeLong = amCloseTime.getTime();
        Long pmOpenTimeLong = pmOpenTime.getTime();
        Long pmCloseTimeLong = pmCloseTime.getTime();
        ArrayList<Long> delayTimeList = new ArrayList<Long>();
        Long amBaseTimeLong = 0l;
        Long pmBaseTimeLong = 0l;
        Long amMaxNum = 0l;
        Long pmMaxNum = 0l;
        if (statDateLong<amCloseTimeLong){
            if (statDateLong<amOpenTimeLong){
                amBaseTimeLong = amOpenTimeLong-statDateLong;
                amMaxNum = (amCloseTimeLong-amOpenTimeLong)/intervalMilliSecond+overTimeUnit;
            }
            else{
                amMaxNum = (amCloseTimeLong-statDateLong)/intervalMilliSecond+overTimeUnit;
            }
            for (long i=0;i<amMaxNum;i++){
                delayTimeList.add(amBaseTimeLong+(i*intervalMilliSecond));
//                System.out.println(amBaseTimeLong+(i*1000));
            }
            pmBaseTimeLong = pmOpenTimeLong-statDateLong;
            pmMaxNum = (pmCloseTimeLong-pmOpenTimeLong)/intervalMilliSecond+overTimeUnit;
            for (long i=0;i<pmMaxNum;i++){
                delayTimeList.add(pmBaseTimeLong+(i*intervalMilliSecond));
//                System.out.println(pmBaseTimeLong+(i*1000));
            }
        }
        else {
            if (statDateLong<pmCloseTimeLong){
                if (statDateLong<pmOpenTimeLong){
                    pmBaseTimeLong = pmOpenTimeLong-statDateLong;
                    pmMaxNum = (pmCloseTimeLong-pmOpenTimeLong)/intervalMilliSecond+overTimeUnit;
                }
                else{
                    pmMaxNum = (pmCloseTimeLong-statDateLong)/intervalMilliSecond+overTimeUnit;
                }
                for (long i=0;i<pmMaxNum;i++){
                    delayTimeList.add(pmBaseTimeLong+(i*intervalMilliSecond));
//                    System.out.println(pmBaseTimeLong+(i*1000));
                }
            }
        }

//        System.out.println(delayTimeList.size());
        return delayTimeList;
    }

    public static Long getCapitalFlowDelayTime(Long statDateLong) throws ParseException {
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
        Long delayTimeLong = 0L;
        Date statDate = new Date();
        statDate.setTime(statDateLong);

        String ymdStr = ymdDF.format(statDate);
        String pmInsertTimeStr = ymdStr+"-154500";
        Date pmInsertTime = fullDF.parse(pmInsertTimeStr);
        Long pmInsertTimeLong = pmInsertTime.getTime();
        if (statDateLong>=pmInsertTimeLong){
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(pmInsertTime);
            calendar.add(Calendar.DAY_OF_YEAR, 1);
            pmInsertTime = calendar.getTime();
            pmInsertTimeLong = pmInsertTime.getTime();
        }
        delayTimeLong = pmInsertTimeLong-statDateLong;
        return delayTimeLong;
    }

    public static void main(String[] args) throws ParseException {
//        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
//        Date statDate = new Date();
//        long statDateLong = statDate.getTime();
////        long statDateLong  = fullDF.parse("20230704-091500").getTime();
////        ArrayList<Long> delayTimeList = DateUtil.getdelayTimeList(statDateLong,300000L,3);
//        ArrayList<Long> delayTimeList = DateUtil.getdelayTimeList(statDateLong,10000L,66);
//        for (Long delayTime:delayTimeList) {
//            long realStatDateLong =statDateLong+delayTime;
//            Date newDate = new Date();
//            newDate.setTime(realStatDateLong);
//            String fullDfStr = fullDF.format(newDate);
//            System.out.println(fullDfStr+"预计");
//        }
    }
}
