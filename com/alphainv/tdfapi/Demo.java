package com.alphainv.tdfapi;

import cn.com.wind.td.tdf.*;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.*;

public class Demo {
//    private final boolean outputToScreen = false;
//    private final boolean outputToScreen = true;
    /***********************configuration***************************************/
    private final String openMarket = "SZ-2-0";
    private final int openTime = 0;
//    private final int openTime = -1;
    private final String subscription = "";// 代码订阅,例如"600000.sh;ag.shf;000001.sz"，需要订阅的股票(单个股票格式为原始Code+.+市场，如999999.SH)，以“;”分割，为空则订阅全市场
//    private final String subscription = "600000.sh;601199.sh;601595.sh;603912.sh;600476.sh;";
    //    private final int openTypeFlags = DATA_TYPE_FLAG.DATA_TYPE_NONE;
    private final int openTypeFlags = DATA_TYPE_FLAG.DATA_TYPE_ORDER|DATA_TYPE_FLAG.DATA_TYPE_TRANSACTION;
//    private final int openTypeFlags = DATA_TYPE_FLAG.DATA_TYPE_TRANSACTION;
    //代码类型订阅，多类型'|'计算，如指数+股票为CODE_TYPE_FLAG.CODE_TYPE_INDEX | CODE_TYPE_FLAG.CODE_TYPE_SHARES
//    private final int openCodeTypeFlags = CODE_TYPE_FLAG.CODE_TYPE_ALL;
    private final int openCodeTypeFlags = CODE_TYPE_FLAG.CODE_TYPE_SHARES|CODE_TYPE_FLAG.CODE_TYPE_INDEX;

    //环境设置参数
    private final int HEART_BEAT_INTERVAL = 0;//Heart Beat间隔（秒数）, 若值为0则表示默认值10秒钟
    private final int MISSED_BEART_COUNT = 0;//如果没有收到心跳次数超过这个值，且没收到其他任何数据，则判断为掉线，若值0为默认次数2次
    private final int OPEN_TIME_OUT = 0;//在调TDF_Open期间，接收每一个数据包的超时时间（秒数，不是TDF_Open调用总的最大等待时间），若值为0则默认30秒
    private final int USE_DISTRIBUTION = 0;//连接的是否为分发服务
    /***********************configuration***************************************/

    private JedisPool[] jedisPoolArray;
    private BlockingQueue<MarketDataInfo> marketDataInfoQueue;
    private BlockingQueue<TransactionDataInfo> transactionDataInfoQueue;
    private BlockingQueue<OrderDataInfo> orderDataInfoQueue;
    private BlockingQueue<IndexDataInfo> indexDataInfoQueue;
    TDFClient client = new TDFClient();
    Demo(String ip, int port, String username, String password, String market, BlockingQueue<MarketDataInfo> marketDataInfoQueue, BlockingQueue<TransactionDataInfo> transactionDataInfoQueue, BlockingQueue<OrderDataInfo> orderDataInfoQueue, BlockingQueue<IndexDataInfo> indexDataInfoQueue) {
        this.marketDataInfoQueue = marketDataInfoQueue;
        this.transactionDataInfoQueue = transactionDataInfoQueue;
        this.orderDataInfoQueue = orderDataInfoQueue;
        this.indexDataInfoQueue = indexDataInfoQueue;
        this.quitFlag = false;
        this.LastPrintTime = System.currentTimeMillis();
        //构造配置参数
        TDF_OPEN_SETTING setting = new TDF_OPEN_SETTING();
        setting.setIp(ip);                                       //服务器IP
        setting.setPort(Integer.toString(port));                //服务器端口
        setting.setUser(username);                               //登录用户名
        setting.setPwd(password);                                //登录密码

        //初始订阅配置
        setting.setMarkets(market);                 //市场列表，不用区分大小写，用英文字符 ; 分割；如果为空，则订阅全部市场。sh;sz;cf;shf;czc;dce;
        setting.setSubScriptions(subscription);         // 代码订阅,例如"600000.sh;ag.shf;000001.sz"，需要订阅的股票(单个股票格式为原始Code+.+市场，如999999.SH)，以“;”分割，为空则订阅全市场
        setting.setTypeFlags(openTypeFlags);            //订阅的数据类型（默认不订阅逐笔成交、逐笔委托和委托队列）参见DATA_TYPE_FLAG
        setting.setTime(openTime);                        //-1表示从头传输，0表示最新行情（默认0）
        setting.setCodeTypeFlags(openCodeTypeFlags);    //订阅的代码类型(默认全部类型订阅)参见CODE_TYPE_FLAG
        //不建议修改配置
        setting.setConnectionID(0);

        //环境设置，在Open之前调用
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_HEART_BEAT_INTERVAL, HEART_BEAT_INTERVAL);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_MISSED_BEART_COUNT, MISSED_BEART_COUNT);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OPEN_TIME_OUT, OPEN_TIME_OUT);
        //setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OUT_LOG,1);//输出日志到当前目录，否则需创建log目录后输出到log目录
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_JNI_MAX_SIZE ,Integer.MAX_VALUE);//API中的消息队列大小，默认为20万，0为无限制

        int err = client.open(setting);                //这里会做连接，登录，收取代码表操作，全部完成后返回（此函数会阻塞一段时间）
        if (err != TDF_ERR.TDF_ERR_SUCCESS) {
            System.out.printf("Can't connect to %s:%d. 程序退出！\n", ip, port);
            System.exit(err);
        }
    }

    //双活模式
    Demo(TDF_SERVER_INFO[] serverInfo) {
        this.quitFlag = false;
        this.LastPrintTime = System.currentTimeMillis();
        /**
         * 服务器设置
         */
        TDF_OPEN_SETTING_EXT setting_ext = new TDF_OPEN_SETTING_EXT();
        setting_ext.setServerInfo(serverInfo);
        setting_ext.setMarkets(openMarket);
        setting_ext.setTime(openTime);
        setting_ext.setSubScriptions(subscription);
        setting_ext.setTypeFlags(openTypeFlags);
        setting_ext.setConnectionID(0);
        setting_ext.setCodeTypeFlags(openCodeTypeFlags);

        //环境设置，在Open之前调用
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_HEART_BEAT_INTERVAL, HEART_BEAT_INTERVAL);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_MISSED_BEART_COUNT, MISSED_BEART_COUNT);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OPEN_TIME_OUT, OPEN_TIME_OUT);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_DISTRIBUTION, USE_DISTRIBUTION);
        //setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OUT_LOG,1);//输出日志到当前目录，否则需创建log目录后输出到log目录

        int err = client.open_ext(setting_ext);                //这里会做连接，登录，收取代码表操作，全部完成后返回（此函数会阻塞一段时间）
        if (err != TDF_ERR.TDF_ERR_SUCCESS) {
            System.out.printf("Can't connect to %s:%s. 程序退出！\n", serverInfo[0].getIp(), serverInfo[0].getPort());
            System.exit(err);
        }
    }

    //代理模式
    Demo(String ip, int port, String username, String password, String proxy_ip, int proxy_port, String proxy_user, String proxy_pwd) {
        this.quitFlag = false;
        this.LastPrintTime = System.currentTimeMillis();
        TDF_OPEN_SETTING setting = new TDF_OPEN_SETTING();
        setting.setIp(ip);
        setting.setPort(Integer.toString(port));
        setting.setUser(username);
        setting.setPwd(password);

        setting.setMarkets(openMarket);
        setting.setTime(openTime);
        setting.setSubScriptions(subscription);
        setting.setTypeFlags(openTypeFlags);
        setting.setConnectionID(0);
        setting.setCodeTypeFlags(openCodeTypeFlags);

        TDF_PROXY_SETTING proxySetting = new TDF_PROXY_SETTING();
        proxySetting.setProxyHostIp(proxy_ip);
        proxySetting.setProxyPort(Integer.toString(proxy_port));
        proxySetting.setProxyUser(proxy_user);
        proxySetting.setProxyPwd(proxy_pwd);
        proxySetting.setProxyType(TDF_PROXY_TYPE.TDF_PROXY_HTTP11);

        //环境设置，在Open之前调用
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_HEART_BEAT_INTERVAL, HEART_BEAT_INTERVAL);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_MISSED_BEART_COUNT, MISSED_BEART_COUNT);
        setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OPEN_TIME_OUT, OPEN_TIME_OUT);
        //setEnv(TDF_ENVIRON_SETTING.TDF_ENVIRON_OUT_LOG,1);//输出日志到当前目录，否则需创建log目录后输出到log目录

        int err = client.openProxy(setting, proxySetting);
        if (err != TDF_ERR.TDF_ERR_SUCCESS) {
            System.out.printf("Can't connect to %s:%d. 程序退出！\n", ip, port);
            System.exit(err);
        }
    }

    protected Boolean quitFlag;
    private long LastPrintTime;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    public int setEnv(int Env, int value) {
        return client.setEnv(Env, value);
    }

    public TDF_OPTION_CODE getOptionCodeInfo(String szCode, String szMarket) {
        return client.getOptionCodeInfo(szCode, szMarket);//szCode格式：原始code + . + 市场(如ag.SHF);szMarket格式:market-level-source(SHF-1-0)
    }

    void printCodeTable(String szMarket) {
        TDF_CODE[] codes = client.getCodeTable(szMarket);
        PrintHelper.printCodeTable(codes);
    }

    void setSusbScription(String szCode, int style) {
        client.setSubscription(szCode, style);// 代码订阅,例如"600000.sh;ag.shf;000001.sz"，需要订阅的股票(单个股票格式为原始Code+.+市场，如999999.SH)，以“;”分割
    }

    public JedisPool[] getJedisPoolArray() {
        return this.jedisPoolArray;
    }

    void run() throws ClassNotFoundException, SQLException, InstantiationException, ParseException, IllegalAccessException, InterruptedException {//这里会循环取数据! 目前JavaAPI的机制是收取的数据存在队列中，等待用户读取。如果用户读取过慢，可能会积压或者丢失数据，所以这里处理要快
        long marketCount = 0;
        long indexCount = 0;
        long transCount = 0;
        long orderCount = 0;
        String printTime = "";
        while (!quitFlag) {
            TDF_MSG msg = client.getMessage(100);       //getMessage是取数据函数。参数含义是：等待多少ms还没数据可读就返回。 如果调用时已有数据可读，立刻返回
            if (msg == null) {
                continue;
            }
            int messageCount = 0;
//            Date startDate = new Date();
//            long startLong = startDate.getTime();
//            SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
            switch (msg.getDataType()) {                           //消息分2类：系统消息（心跳，网络断开，网络连接结果，收到登陆响应，收到代码表，收到闭市，收到行情变更）
                //数据消息（行情，指数，期货，逐笔成交，逐笔委托，委托队列）。 使用getDataType获取消息类型
                //系统消息
                case TDF_MSG_ID.MSG_SYS_HEART_BEAT:
                    System.out.println("收到心跳信息！");
                    break;
                case TDF_MSG_ID.MSG_SYS_DISCONNECT_NETWORK:
                    System.out.println("网络断开！");
                    //quitFlag = true;
                    break;
                case TDF_MSG_ID.MSG_SYS_CONNECT_RESULT: {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);                  //getMessageData: TDF_MSG可能包含多条对应类型的消息，使用此函数取条N条
                    System.out.println("网络连接结果：");
                    PrintHelper.printConnectResult(data.getConnectResult());               //再使用对应方法取具体数据
                    break;
                }
                case TDF_MSG_ID.MSG_SYS_LOGIN_RESULT: {
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                    PrintHelper.printLoginResult(data.getLoginResult());
                    break;
                }
                case TDF_MSG_ID.MSG_SYS_CODETABLE_RESULT: {
                    System.out.println("收到代码表！");
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                    PrintHelper.printCodeTableResult(data.getCodeTableResult());
                    //printCodeTable(data.getCodeTableResult().getMarket()[0]);
                    break;
                }
                case TDF_MSG_ID.MSG_SYS_SINGLE_CODETABLE_RESULT: {
                    TDF_SINGLE_CODE_RESULT data = TDFClient.getMessageData(msg, 0).getSingleCodeTableResult();
                    String market = data.getMarket();
                    System.out.println("收到单个市场代码表通知,市场=" + market + ",日期=" + data.getCodeDate() + ",代码数=" + data.getCodeCount());
                    break;
                }
                case TDF_MSG_ID.MSG_SYS_ADDCODE: {
                    System.out.println("收到新增代码！");
                    TDF_MSG_DATA data = TDFClient.getMessageData(msg, 0);
                    PrintHelper.printAddCode(data.getAddCode());
                    break;
                }
                //数据消息
                case TDF_MSG_ID.MSG_DATA_MARKET:
                    messageCount = msg.getAppHead().getItemCount();
                    for (int i = 0; i < messageCount; i++) {
                        TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                        TDF_MARKET_DATA market_data_instance = data.getMarketData();
                        String exchangeTimeStrMs = String.valueOf(market_data_instance.getTime());
                        MarketDataInfo marketDataInfoInstance = new MarketDataInfo(market_data_instance.getHighLimited(),
                                market_data_instance.getLowLimited(),
                                market_data_instance.getMatch(),
                                market_data_instance.getPreClose(),
                                exchangeTimeStrMs,
                                market_data_instance.getCode());
                        marketDataInfoQueue.put(marketDataInfoInstance);
                        marketCount++;
                        printTime =exchangeTimeStrMs;
                    }
                    if (marketCount%200000==0){
                        System.out.println("已从交易所接收"+marketCount+" 当前market队列长度还剩"+marketDataInfoQueue.size()+" 当前交易所时间" +printTime);
                    }
                    break;
                case TDF_MSG_ID.MSG_DATA_INDEX:
                    messageCount = msg.getAppHead().getItemCount();
                    for (int i = 0; i < messageCount; i++) {
                        TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                        TDF_INDEX_DATA index_data_instance = data.getIndexData();
                        String exchangeTimeStrMs = String.valueOf(index_data_instance.getTime());
                        IndexDataInfo indexDataInfoInstance = new IndexDataInfo(index_data_instance.getHighIndex(),
                                index_data_instance.getLowIndex(),
                                index_data_instance.getOpenIndex(),
                                index_data_instance.getLastIndex(),
                                index_data_instance.getPreCloseIndex(),
                                exchangeTimeStrMs,
                                index_data_instance.getCode());
                        indexDataInfoQueue.put(indexDataInfoInstance);
                        indexCount++;
                        printTime =exchangeTimeStrMs;
                    }
                    if (indexCount%200000==0){
                        System.out.println("已从交易所接收"+indexCount+" 当前index队列长度还剩"+indexDataInfoQueue.size()+" 当前交易所时间" +printTime);
                    }
                    break;
                case TDF_MSG_ID.MSG_DATA_TRANSACTION:
                    messageCount = msg.getAppHead().getItemCount();
                    for (int i = 0; i <messageCount; i++) {
                        TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                        TDF_TRANSACTION transData = data.getTransaction();
                        String exchangeTimeStrMs = String.valueOf(transData.getTime());
                        TransactionDataInfo transactionDataInfoInstance = new TransactionDataInfo(
                                exchangeTimeStrMs,
                                transData.getCode(),
                                transData.getFunctionCode(),
                                transData.getTurnover(),
                                transData.getVolume(),
                                transData.getChannel(),
                                transData.getAskOrder(),
                                transData.getBidOrder(),
                                transData.getBSFlag()
                        );
                        transactionDataInfoQueue.put(transactionDataInfoInstance);
                        transCount++;
                        printTime =exchangeTimeStrMs;
                    }
                    if (transCount%200000==0){
                        System.out.println("已从交易所接收"+transCount+" 当前transaction队列长度还剩"+transactionDataInfoQueue.size()+" 当前trans交易所时间" +printTime);
                        //三线程取数据 已从交易所接收4200000 当前transaction队列长度还剩744909 当前trans交易所时间93132060
                    }
                    break;
                case TDF_MSG_ID.MSG_DATA_ORDER:
                    messageCount = msg.getAppHead().getItemCount();
                    for (int i = 0; i < messageCount; i++) {
                        TDF_MSG_DATA data = TDFClient.getMessageData(msg, i);
                        TDF_ORDER order_data = data.getOrder();
                        String exchangeTimeStrMs = String.valueOf(order_data.getTime());
                        OrderDataInfo orderDataInfoInstance = new OrderDataInfo(
                                exchangeTimeStrMs,
                                order_data.getCode(),
                                order_data.getFunctionCode(),
                                order_data.getOrderKind(),
                                order_data.getChannel(),
                                order_data.getVolume(),
                                order_data.getOrder(),
                                order_data.getPrice(),
                                order_data.getOrderOriNo()
                        );
                        orderDataInfoQueue.put(orderDataInfoInstance);
                        orderCount++;
                        printTime =exchangeTimeStrMs;
                    }
                    if (orderCount%200000==0){
                        System.out.println("已从交易所接收"+orderCount+" 当前 order队列长度还剩"+orderDataInfoQueue.size()+" 当前交易所时间" +printTime);
                    }
                    break;
                default:
                    break;
            }
//            if (messageCount>1){
//                Date endDate = new Date();
//                long endLong = endDate.getTime();
//                long duraMs = endLong-startLong;
//                System.out.println("已接收"+messageCount+" 耗时： "+duraMs+"毫秒 开始时间："+fullDF.format(startDate)+" 结束时间："+fullDF.format(endDate));
//            }

        }
        client.close();
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, ParseException, IllegalAccessException, InterruptedException {
        boolean testSub = false;//test for subscription
        if (args.length != 5) {
            System.out.println("usage:  Demo ip port user password market");
            System.exit(1);
        }


        BlockingQueue<MarketDataInfo> marketDataInfoQueue = new LinkedBlockingQueue <MarketDataInfo>();
        BlockingQueue<TransactionDataInfo> transactionDataInfoQueue = new LinkedBlockingQueue <TransactionDataInfo>();
        BlockingQueue<OrderDataInfo> orderDataInfoQueue = new LinkedBlockingQueue <OrderDataInfo>();
        BlockingQueue<IndexDataInfo> indexDataInfoQueue = new LinkedBlockingQueue<IndexDataInfo>();
        // Proxy Mode
		/*Demo demo = new Demo("10.100.3.163", 20200, "dev_test", "dev_test", 
				"10.100.6.125", 3128, "", "");*/
        //命令行输入模式
        Demo demo = new Demo(args[0], Integer.parseInt(args[1]), args[2], args[3], args[4], marketDataInfoQueue, transactionDataInfoQueue, orderDataInfoQueue,indexDataInfoQueue);  //这里会打开到Server的连接。具体看实现代码
//        demo.setSusbScription("600680.SH;000002.sz", SUBSCRIPTION_STYLE.SUBSCRIPTION_SET);
        //一般配置模式
        //Demo demo = new Demo("10.100.3.67", 6221, "dev_test", "dev_test", "SZ-2-0");
//        三分钟
//        当前transaction队列长度还剩66227
//        当前 order队列长度还剩107405
//        当前market队列长度还剩13570

//        当前market队列长度还剩0
//        当前 order队列长度还剩67
//        当前transaction队列长度还剩34962
        /*
         * 双（多）活模式
         */
		/*
		int MaxServerNum = 2;//需要使用的连接数
		TDF_SERVER_INFO[] serverInfo = new TDF_SERVER_INFO[MaxServerNum];
		for(int i = 0; i < MaxServerNum; i++)
			serverInfo[i] = new TDF_SERVER_INFO();
		//双活以下方式配置
		serverInfo[0].setIp("10.100.3.163");
		serverInfo[0].setPort("20201");
		serverInfo[0].setUser("dev_test");
		serverInfo[0].setPwd("dev_test");
	
		serverInfo[1].setIp("10.100.2.199");
		serverInfo[1].setPort("6221");
		serverInfo[1].setUser("dev_test");
		serverInfo[1].setPwd("dev_test");
		//连接
		Demo demo = new Demo(serverInfo);
		//双活结束
		*/
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(20);
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig,"127.0.0.1",6476,3000);
        DataHandler dh = new DataHandler(demo);
        String[] channelArray = new String[]{"1","2","3","4","5","6","20","2011","2012","2013","2014"};
        HashMap<String,Integer> channelCodeMap = new HashMap<String,Integer>();
        ChannelQueueData[] channelQueueDataArray = new ChannelQueueData[11];
        for (int i=0;i<channelArray.length;i++){
            channelCodeMap.put(channelArray[i],i);
            ChannelQueueData channelQueueDataInstance = new ChannelQueueData(channelArray[i]);
            channelQueueDataArray[i]=channelQueueDataInstance;
        }
//        ArrayList<String> allStkList = DataBaseOperation.getallStkList();

        HashMap<String,String> stkChannelMap = new HashMap<String,String>();
        ConcurrentHashMap<String,String> stkSDMapAll = new ConcurrentHashMap<String,String>();
        ConcurrentHashMap<String,Double> indexSDMapAll = new ConcurrentHashMap<String,Double>();
        ConcurrentHashMap<String,Long> orderBuyMapAll = new ConcurrentHashMap<String,Long>();
        ConcurrentHashMap<String,Long> orderSellMapAll = new ConcurrentHashMap<String,Long>();
        ConcurrentHashMap<String,Double> weightedOrderBuyMapAll = new ConcurrentHashMap<String,Double>();
        ConcurrentHashMap<String,Double> weightedOrderSellMapAll = new ConcurrentHashMap<String,Double>();
        ConcurrentHashMap<String,Long> order5MBuyMapAll = new ConcurrentHashMap<String,Long>();
        ConcurrentHashMap<String,Long> order5MSellMapAll = new ConcurrentHashMap<String,Long>();
        ConcurrentHashMap<String,Long> transBuyMapAll = new ConcurrentHashMap<String,Long>();
        ConcurrentHashMap<String,Long> transSellMapAll = new ConcurrentHashMap<String,Long>();
        ConcurrentHashMap<String,Double> weightedOrderBSRateMapAll = new ConcurrentHashMap<String,Double>();
        ConcurrentHashMap<String,Double> orderBsRateMapALL = new ConcurrentHashMap<String,Double>();
        ConcurrentHashMap<String,Double> transBsRateMapALL = new ConcurrentHashMap<String,Double>();
        ConcurrentHashMap<String, ArrayList<Double>> compositeScoreMapALL = new ConcurrentHashMap<String, ArrayList<Double>>();
        ConcurrentHashMap<String,Integer> highLimitFlagMapAll = new ConcurrentHashMap<String,Integer>();

        TaskQueueDaemonThread taskQueueDaemonThread = TaskQueueDaemonThread.getInstance();
        taskQueueDaemonThread.init();
        taskQueueDaemonThread.put(0,dh);

        MarketDataInfoWrite marketDataInfoWriteInstance = new MarketDataInfoWrite(marketDataInfoQueue,channelQueueDataArray,channelCodeMap,stkChannelMap,jedisPool,highLimitFlagMapAll);
        taskQueueDaemonThread.put(0,marketDataInfoWriteInstance);

        IndexDataInfoWrite indexDataInfoWriteInstance = new IndexDataInfoWrite(indexDataInfoQueue,indexSDMapAll);
        taskQueueDaemonThread.put(0,indexDataInfoWriteInstance);

        OrderDataInfoWrite orderDataInfoWriteInstance = new OrderDataInfoWrite(channelQueueDataArray,orderDataInfoQueue,channelCodeMap,stkChannelMap);
        taskQueueDaemonThread.put(10000,orderDataInfoWriteInstance);

        TransactionDataInfoWrite transactionDataInfoWriteInstance = new TransactionDataInfoWrite(channelQueueDataArray, transactionDataInfoQueue, channelCodeMap);
        taskQueueDaemonThread.put(10000,transactionDataInfoWriteInstance);

        for (int i=0;i<channelArray.length;i++){
            ChannelQueueData channelQueueDataInstance = channelQueueDataArray[i];
            BlockingQueue<Object[]> orderBuyQueue = channelQueueDataInstance.getOrderBuyQueue();
            BlockingQueue<Object[]> orderSellQueue = channelQueueDataInstance.getOrderSellQueue();
            ConcurrentHashMap<String,Double> orderBuyWeightedMap = channelQueueDataInstance.getOrderBuyWeightedMap();
            ConcurrentHashMap<String,Double> orderSellWeightedMap = channelQueueDataInstance.getOrderSellWeightedMap();
            DelayQueue<DelayedOrder> m5OrderBuyDelayQueue = channelQueueDataInstance.getM5OrderBuyDelayQueue();
            DelayQueue<DelayedOrder> m5OrderSellDelayQueue = channelQueueDataInstance.getM5OrderSellDelayQueue();
            BlockingQueue<Object[]> transBuyQueue = channelQueueDataInstance.getTransBuyQueue();
            BlockingQueue<Object[]> transSellQueue = channelQueueDataInstance.getTransSellQueue();
            ConcurrentHashMap<String,Long> newestPriceMap = channelQueueDataInstance.getNewestPriceMap();
            ConcurrentHashMap<String,Integer> highLimitFlagMap = channelQueueDataInstance.getHighLimitFlagMap();
            ConcurrentHashMap<String,Integer> lowLimitFlagMap = channelQueueDataInstance.getLowLimitFlagMap();
            ConcurrentHashMap<String,Long> orderBuyMap = channelQueueDataInstance.getOrderBuyMap();
            ConcurrentHashMap<String,Long> orderSellMap = channelQueueDataInstance.getOrderSellMap();
            ConcurrentHashMap<String,Long> order5MBuyMap = channelQueueDataInstance.getOrder5MBuyMap();
            ConcurrentHashMap<String,Long> order5MSellMap = channelQueueDataInstance.getOrder5MSellMap();
            ConcurrentHashMap<String,Long> transBuyMap = channelQueueDataInstance.getTransBuyMap();
            ConcurrentHashMap<String,Long> transSellMap = channelQueueDataInstance.getTransSellMap();
            ConcurrentHashMap<String,Long> orderBuyTrueMap = channelQueueDataInstance.getOrderBuyTrueMap();
            ConcurrentHashMap<String,Long> orderSellTrueMap = channelQueueDataInstance.getOrderSellTrueMap();
            ConcurrentHashMap<String,Double> orderBuyWeightedTrueMap = channelQueueDataInstance.getOrderBuyWeightedTrueMap();
            ConcurrentHashMap<String,Double> orderSellWeightedTrueMap = channelQueueDataInstance.getOrderSellWeightedTrueMap();
            ConcurrentHashMap<String,Long> order5MBuyTrueMap = channelQueueDataInstance.getOrder5MBuyTrueMap();
            ConcurrentHashMap<String,Long> order5MSellTrueMap = channelQueueDataInstance.getOrder5MSellTrueMap();


            ChannelStockBuyOrderStat channelStockBuyOrderStat = new ChannelStockBuyOrderStat(orderBuyQueue,newestPriceMap,lowLimitFlagMap,
                    orderBuyMap,orderBuyWeightedMap,order5MBuyMap,
                    orderBuyTrueMap,orderBuyWeightedTrueMap,order5MBuyTrueMap
            );
            ChannelStockSellOrderStat channelStockSellOrderStat = new ChannelStockSellOrderStat(orderSellQueue,newestPriceMap,highLimitFlagMap,
                    orderSellMap,orderSellWeightedMap,order5MSellMap,
                    orderSellTrueMap,orderSellWeightedTrueMap,order5MSellTrueMap
            );
            ChannelStock5MBuyOrderStat channelStock5MBuyOrderStat = new ChannelStock5MBuyOrderStat(m5OrderBuyDelayQueue,order5MBuyMap);
            ChannelStock5MSellOrderStat channelStock5MSellOrderStat = new ChannelStock5MSellOrderStat(m5OrderSellDelayQueue,order5MSellMap);
            ChannelStockBuyTransStat channelStockBuyTransStat = new ChannelStockBuyTransStat(transBuyQueue,transBuyMap);
            ChannelStockSellTransStat channelStockSellTransStat = new ChannelStockSellTransStat(transSellQueue,transSellMap);
            taskQueueDaemonThread.put(0,channelStockBuyOrderStat);
            taskQueueDaemonThread.put(0,channelStockSellOrderStat);
            taskQueueDaemonThread.put(0,channelStock5MBuyOrderStat);
            taskQueueDaemonThread.put(0,channelStock5MSellOrderStat);
            taskQueueDaemonThread.put(0,channelStockBuyTransStat);
            taskQueueDaemonThread.put(0,channelStockSellTransStat);
        }

//        ExecutorService executorService = taskQueueDaemonThread.getExecutorService();
//        executorService.execute(sdw);

        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd-HHmmss");
        Date statDate = new Date();
        long statDateLong = statDate.getTime();
//        statDateLong = fullDF.parse("20230619-091500").getTime();
        ArrayList<Long> delayTimeList = DateUtil.getdelayTimeList(statDateLong,10000L,66);
//        statDateLong = statDateLong-5000;

        for (Long delayTime:delayTimeList) {
            long realStatDateLong =statDateLong+delayTime;
            Date newDate = new Date();
            newDate.setTime(realStatDateLong);
            String fullDfStr = fullDF.format(newDate);
            System.out.println(fullDfStr+"预计");
            AllStockStat allStockStat = new AllStockStat(stkChannelMap,channelQueueDataArray,channelCodeMap,
                    jedisPool,channelArray,stkSDMapAll,
                    orderBuyMapAll,orderSellMapAll,
                    weightedOrderBuyMapAll,weightedOrderSellMapAll,
                    order5MBuyMapAll,order5MSellMapAll,
                    transBuyMapAll,transSellMapAll,
                    orderBsRateMapALL,weightedOrderBSRateMapAll, transBsRateMapALL, compositeScoreMapALL,indexSDMapAll);
            taskQueueDaemonThread.put(delayTime,allStockStat);
        }
        JedisPoolConfig sectorJedisPoolConfig = new JedisPoolConfig();
        sectorJedisPoolConfig.setMaxTotal(5);
        JedisPool sectorJedisPool = new JedisPool(sectorJedisPoolConfig,"127.0.0.1",6476,3000);
        AllSectorStat allSectorStat = new AllSectorStat(sectorJedisPool,stkSDMapAll,
                orderBuyMapAll,orderSellMapAll,
                weightedOrderBuyMapAll,weightedOrderSellMapAll,
                order5MBuyMapAll,order5MSellMapAll,
                transBuyMapAll,transSellMapAll);
        taskQueueDaemonThread.put(0,allSectorStat);

        ArrayList<Long> dbDelayTimeList = DateUtil.getdelayTimeList(statDateLong,300000L,2);
//        从9：35开始往oracle保存数据
        for (int i = 4; i < dbDelayTimeList.size(); i++) {
            Long delayTime = dbDelayTimeList.get(i);
            long realStatDateLong =statDateLong+delayTime;
            Date newDate = new Date();
            newDate.setTime(realStatDateLong);
            String fullDfStr = fullDF.format(newDate);
            System.out.println(fullDfStr+"预计进行Oracle操作");
            StockRankStat stockRankStat = new StockRankStat(weightedOrderBSRateMapAll,orderBsRateMapALL,transBsRateMapALL,compositeScoreMapALL,highLimitFlagMapAll,fullDfStr);
            taskQueueDaemonThread.put(delayTime,stockRankStat);
        }

        //订阅
        if (testSub) {
            demo.setSusbScription("600680.SH;000002.sz", SUBSCRIPTION_STYLE.SUBSCRIPTION_SET);
        }

//        System.out.println("press anything to quit the program.");
//        try {
//            System.in.read();
//            demo.setQuitFlag(true);
//            sdw.setQuitFlag(true);
//            t1.join();
//            System.out.println("Thread1 Quit!");
//            t2.join();
//            System.out.println("Thread2 Quit!");
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("Quit the program!");
//        System.exit(0);
    }
}


class DataHandler implements Runnable {
    protected Demo demo;

    public DataHandler(Demo d) {
        this.demo = d;
    }

    public void run() {
        try {
            demo.run();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class DataWrite implements Runnable{
    private static final int WRITE_GAP = 5;
    private static final int LIST_LEN = 20000;
    private ArrayList<String> statPortfolioStocks;
    private Integer redisIndex;
    private long statDateLong;
    private int statThreadIndex;
    JedisPool[] jedisPoolArray;

    public DataWrite(long statDateLong, ArrayList<String> portfolioStocks, JedisPool[] jedisPoolArray, Integer redisIndex) {
        quitFlag = false;
//        System.out.println("传入时间："+ statDateLong);
        this.statDateLong = statDateLong;
        this.statPortfolioStocks = portfolioStocks;
        this.jedisPoolArray = jedisPoolArray;
        this.redisIndex = redisIndex;
    }

//    private Demo demo;
    protected Boolean quitFlag;
//    private long lastWriteTime;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;

    }

    public void run() {
        try {
            StockStat statInstance = new StockStat();
//            System.out.println("传入时间："+ statDateLong);
            statInstance.getStockStat(statDateLong, statPortfolioStocks,jedisPoolArray,redisIndex);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}

class SectorDataWrite implements Runnable {

    private HashMap<String,Integer> stockRedisIndexMap;

    private HashMap<String,Integer> sectorRedisIndexMap;

    private HashMap<String,ArrayList<String>> swSectorListMap;

    private HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection;

    JedisPool[] jedisPoolArray;

    public SectorDataWrite(HashMap<String,Integer> stockIndexMap, HashMap<String,Integer> sectorIndexMap, JedisPool[] jedisPoolArray) {
        quitFlag = false;
        this.sectorRedisIndexMap = sectorIndexMap;
        this.stockRedisIndexMap = stockIndexMap;
        this.jedisPoolArray = jedisPoolArray;
        try {
            this.swSectorListMap = DataBaseOperation.getSwSectorListMap();
            this.sectorStkMapCollection = DataBaseOperation.getSectorStkMapCollection(swSectorListMap);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    protected Boolean quitFlag;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;

    }
    public void run() {
        while (!quitFlag) {
            try {
                ArrayList<String> sectorClassList = new ArrayList<String>();
                sectorClassList.add("sw1");
                sectorClassList.add("sw2");
                sectorClassList.add("sw3");
                SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd HHmmss");
                for (String sectorClass:sectorClassList) {
                    SectorStat.getSectorStat(stockRedisIndexMap,sectorRedisIndexMap,sectorClass,swSectorListMap,sectorStkMapCollection,jedisPoolArray);
                    Date finishTime = new Date();
                    System.out.println("已在 "+fullDF.format(finishTime)+" 完成更新行业"+sectorClass);
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
