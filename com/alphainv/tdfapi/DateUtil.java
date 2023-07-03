package com.alphainv.tdfapi;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class DateUtil {



    public static ArrayList<Long> getdelayTimeList(Long statDateLong, Long intervalMilliSecond) throws ParseException {
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
                amMaxNum = (amCloseTimeLong-amOpenTimeLong)/intervalMilliSecond+3;
            }
            else{
                amMaxNum = (amCloseTimeLong-statDateLong)/intervalMilliSecond+3;
            }
            for (long i=0;i<amMaxNum;i++){
                delayTimeList.add(amBaseTimeLong+(i*intervalMilliSecond));
//                System.out.println(amBaseTimeLong+(i*1000));
            }
            pmBaseTimeLong = pmOpenTimeLong-statDateLong;
            pmMaxNum = (pmCloseTimeLong-pmOpenTimeLong)/intervalMilliSecond+3;
            for (long i=0;i<pmMaxNum;i++){
                delayTimeList.add(pmBaseTimeLong+(i*intervalMilliSecond));
//                System.out.println(pmBaseTimeLong+(i*1000));
            }
        }
        else {
            if (statDateLong<pmCloseTimeLong){
                if (statDateLong<pmOpenTimeLong){
                    pmBaseTimeLong = pmOpenTimeLong-statDateLong;
                    pmMaxNum = (pmCloseTimeLong-pmOpenTimeLong)/intervalMilliSecond+3;
                }
                else{
                    pmMaxNum = (pmCloseTimeLong-statDateLong)/intervalMilliSecond+3;
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

    public static void main(String[] args) throws ParseException {
//        getdelayTimeList();
//        System.out.println(delayTimeList.size());
//        System.out.println(fullDF.format(new Date()));
    }
}
