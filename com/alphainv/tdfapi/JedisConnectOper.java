package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;

public class JedisConnectOper {

    public static void main(String[] args){
        Jedis jedisInstance = new Jedis("127.0.0.1",6464);
        System.out.println(jedisInstance.ping());

        jedisInstance.select(1);
//        jedisInstance.set("key1","value1");
//        jedisInstance.set("key2","value2");
//        System.out.println(jedisInstance.get("key1"));
//        System.out.println(jedisInstance.get("key2"));
//        jedisInstance.lpush("collections", "ArrayList", "Vector", "Stack", "HashMap", "WeakHashMap", "LinkedHashMap");
//        jedisInstance.lpush("collections", "HashSet");
//        jedisInstance.lpush("collections", "TreeSet");
//        jedisInstance.lpush("collections", "TreeMap");
        System.out.println("collections的内容："+jedisInstance.lrange("collections", 0, -1));
        String[] contents = {"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
                "成交编号","成交价格","成交数量","成交金额","买卖方向","成交类别","成交代码",
                "卖方委托序号","买方委托序号", "Channel ID","BizIndex"};
        System.out.println(contents.toString());
    }

}
