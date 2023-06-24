package com.alphainv.tdfapi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MapOrderByValueUtil {


    public static List<Map.Entry<String,Double>> getMapOrderByValue(ConcurrentHashMap<String,Double> originalMap){
        List<Map.Entry<String,Double>> list = new ArrayList<>(originalMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String,Double>>()
        {
            @Override
            public int compare(Map.Entry<String,Double> o1, Map.Entry<String,Double> o2) {
                //按照value值升序
//                return o1.getValue() - o2.getValue();
                //按照value值降序
                //return o2.getValue() - o1.getValue();
                Double o1Value = o1.getValue();
                Double o2Value = o2.getValue();
                if (o2Value>o1Value){
                    return 1;
                }
                else if (o2Value<o1Value){
                    return -1;
                }
                else {
                    return 0;
                }
            }
        });
        return list;
    }

    public static List<Map.Entry<String,ArrayList<Double>>> getMapOrderByListValue(ConcurrentHashMap<String, ArrayList<Double>> originalMap){
        List<Map.Entry<String,ArrayList<Double>>> list = new ArrayList<>(originalMap.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String,ArrayList<Double>>>()
        {
            @Override
            public int compare(Map.Entry<String,ArrayList<Double>> o1, Map.Entry<String,ArrayList<Double>> o2) {
                //按照value值升序
//                return o1.getValue() - o2.getValue();
                //按照value值降序
                //return o2.getValue() - o1.getValue();
                Double o1Value = o1.getValue().get(2);
                Double o2Value = o2.getValue().get(2);
                if (o2Value>o1Value){
                    return 1;
                }
                else if (o2Value<o1Value){
                    return -1;
                }
                else {
                    return 0;
                }
            }
        });
        return list;
    }

    public static void main(String[] args) {
//        ConcurrentHashMap<String,Double> originalMap = new ConcurrentHashMap<>();
//        originalMap.put("000001",42.63);
//        originalMap.put("000002",40.63);
//        originalMap.put("000003",41.63);
//        originalMap.put("000004",41.63);
//        originalMap.put("000005",0.0);
//        originalMap.put("000006",40.63);
//        for (Map.Entry<String,Double> c:getMapOrderByValue(originalMap)) {
//            System.out.println(c.getKey() +" : " + c.getValue());
//        }
//
//        ConcurrentHashMap<String, ArrayList<Double>> originalMap2 = new ConcurrentHashMap<>();
//        ArrayList<Double> a1 = new ArrayList<>();
//        a1.add(0D);
//        a1.add(0D);
//        a1.add(42.63);
//        originalMap2.put("000001",a1);
//        ArrayList<Double> a2 = new ArrayList<>();
//        a2.add(0D);
//        a2.add(0D);
//        a2.add(40.63);
//        originalMap2.put("000002",a2);
//        ArrayList<Double> a3 = new ArrayList<>();
//        a3.add(0D);
//        a3.add(0D);
//        a3.add(41.63);
//        originalMap2.put("000003",a3);
//        ArrayList<Double> a4 = new ArrayList<>();
//        a4.add(0D);
//        a4.add(0D);
//        a4.add(41.63);
//        originalMap2.put("000004",a4);
//        ArrayList<Double> a5 = new ArrayList<>();
//        a5.add(0D);
//        a5.add(0D);
//        a5.add(0.0);
//        originalMap2.put("000005",a5);
//        ArrayList<Double> a6 = new ArrayList<>();
//        a6.add(0D);
//        a6.add(0D);
//        a6.add(40.63);
//        originalMap2.put("000006",a6);
//        for (Map.Entry<String,ArrayList<Double>> c:getMapOrderByListValue(originalMap2)) {
//            System.out.println(c.getKey() +" : " + c.getValue().get(2));
//        }
    }
}
