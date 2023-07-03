package com.alphainv.tdfapi;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

public class TransactionDataInfoWrite implements Runnable{
    protected Boolean quitFlag = false;
    BlockingQueue<TransactionDataInfo> transactionDataInfoQueue;
    ChannelQueueData[] channelQueueDataArray;
    HashMap<String,Integer> channelCodeMap;

    TransactionDataInfoWrite(ChannelQueueData[] channelQueueDataArray, BlockingQueue<TransactionDataInfo> transactionDataInfoQueue, HashMap<String,Integer> channelCodeMap){
        this.channelQueueDataArray = channelQueueDataArray;
        this.transactionDataInfoQueue = transactionDataInfoQueue;
        this.channelCodeMap = channelCodeMap;
    }

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    @Override
    public void run() {
        Long countNum = 0l;
//        Long refillNum = 0l;
        while (!quitFlag){
            TransactionDataInfo transactionDataInfoInstance = transactionDataInfoQueue.poll();
            if (countNum%200000==0){
                int queueSize = transactionDataInfoQueue.size();
                System.out.println("已处理"+countNum+", 当前transaction队列长度还剩"+queueSize);
            }
//            if (refillNum%200000==0){
//                System.out.println("已回填"+refillNum);
//            }
            if (transactionDataInfoInstance!=null){
                String singleStockCodeNum = transactionDataInfoInstance.getStockCode();
                if (singleStockCodeNum.startsWith("6")||singleStockCodeNum.startsWith("0")||singleStockCodeNum.startsWith("3")){
                    char stkFunctionCode = transactionDataInfoInstance.getStkFunctionCode();
                    Long turnOver = transactionDataInfoInstance.getTurnOver();
//               深交所统不在逐笔委托中发送撤单，撤单成功的回报消息是在逐笔成交中发送。
//               通过委买，委卖编号和ChannelID可以与逐笔委托对应
//               如果通过委卖编号和ChannelID可以找到对应的逐笔委托数据，则可以直接从当前买单卖单中减去这笔数据
//               如果通过使用委卖编号和ChannelI找不到对应的逐笔委托数据，则使用当前价格乘以下单股数（即为VolumeNum）作为减去的数据
                    int volumeNum = transactionDataInfoInstance.getVolumeNum();
                    Integer channelId = transactionDataInfoInstance.getChannelId();
//                                    卖方委托序号
                    int askOrderNum = transactionDataInfoInstance.getAskOrderNum();
//                                    买方委托序号
                    int bidOrderNum = transactionDataInfoInstance.getBidOrderNum();
                    Integer channelIndex = null;
                    if (channelId!=null){
                        channelIndex=channelCodeMap.get(String.valueOf(channelId));
                    }
                    if (channelIndex!=null){
                        ChannelQueueData channelQueueData = channelQueueDataArray[channelIndex];
                        BlockingQueue<Object[]> orderBuyQueue = channelQueueData.getOrderBuyQueue();
                        BlockingQueue<Object[]> orderSellQueue = channelQueueData.getOrderSellQueue();
                        BlockingQueue<Object[]> transBuyQueue = channelQueueData.getTransBuyQueue();
                        BlockingQueue<Object[]> transSellQueue = channelQueueData.getTransSellQueue();
                        Long[] orderPriceArray = channelQueueData.getOrderPriceArray();
                        Double[] orderSZPriceWeightArray = channelQueueData.getOrderSZPriceWeightArray();
                        Object[] orderInfoArray = new Object[3];
                        orderInfoArray[0]=singleStockCodeNum;
                        Object[] transInfoArray = new Object[2];
                        transInfoArray[0]=singleStockCodeNum;
                        transInfoArray[1]=turnOver;
//                                深交所撤单数据
                        if (stkFunctionCode == 'C'){
                            int orderNum = 0;
                            if (askOrderNum==0){
                                orderNum = bidOrderNum;
                            }
                            else{
                                orderNum = askOrderNum;
                            }
                            Long orderPrice =  orderPriceArray[orderNum];
                            Long orderTurnover = 0l;
                            if (orderPrice==null){
                                transactionDataInfoQueue.offer(transactionDataInfoInstance);
//                            System.out.println(
//                                    "stkCode: "+ singleStockCodeNum+
//                                            " stkFunctionCode: "+ singleStockCodeNum+
//                                            " turnOver " + turnOver+
//                                            " volumeNum " + volumeNum+
//                                            " channelId " + channelId+
//                                            " askOrderNum" + askOrderNum+
//                                            " bidOrderNum " + bidOrderNum
//                            );
//                            refillNum++;
                                continue;
                            }
                            else{
                                orderTurnover=-1*orderPrice*volumeNum;

                            }
                            Double orderPriceWeight = orderSZPriceWeightArray[orderNum];
                            if (orderPriceWeight==null){
                                transactionDataInfoQueue.offer(transactionDataInfoInstance);
                                continue;
                            }
                            Double weightedTurnoverValue = orderPriceWeight*orderTurnover;
                            orderInfoArray[1]=orderTurnover;
                            orderInfoArray[2]=weightedTurnoverValue;
                            if (askOrderNum==0){
                                try {
                                    orderBuyQueue.put(orderInfoArray);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                            else{
                                try {
                                    orderSellQueue.put(orderInfoArray);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }

                        }
                        else{
//              接收到深交所股票成交数据后，将成交单对应的原始买单卖单从当前的买单和卖单中减去
                            if(!singleStockCodeNum.startsWith("6")){
                                Long buyOrderPrice = orderPriceArray[bidOrderNum];
                                Long sellOrderPrice = orderPriceArray[askOrderNum];
                                Double buyOrderPriceWeight = orderSZPriceWeightArray[bidOrderNum];
                                Double sellOrderPriceWeight = orderSZPriceWeightArray[askOrderNum];
                                if (null!=buyOrderPrice&&null!=sellOrderPrice&&null!=buyOrderPriceWeight&&null!=sellOrderPriceWeight){
                                    Object[] orderInfoArrayTB = new Object[3];
                                    orderInfoArrayTB[0]=singleStockCodeNum;
                                    Long buyOrderTurnOver = -1*buyOrderPrice*volumeNum;
                                    orderInfoArrayTB[1]=buyOrderTurnOver;
                                    orderInfoArrayTB[2]=buyOrderPriceWeight*buyOrderTurnOver;

                                    Object[] orderInfoArrayTS = new Object[3];
                                    orderInfoArrayTS[0]=singleStockCodeNum;
                                    Long sellOrderTurnOver = -1*sellOrderPrice*volumeNum;
                                    orderInfoArrayTS[1]=sellOrderTurnOver;
                                    orderInfoArrayTS[2]=sellOrderPriceWeight*sellOrderTurnOver;
                                    try {
                                        orderBuyQueue.put(orderInfoArrayTB);
                                        orderSellQueue.put(orderInfoArrayTS);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                                else {
                                    transactionDataInfoQueue.offer(transactionDataInfoInstance);
//                                System.out.println(
//                                        "stkCode: "+ singleStockCodeNum+
//                                                " stkFunctionCode: "+ singleStockCodeNum+
//                                                " turnOver " + turnOver+
//                                                " volumeNum " + volumeNum+
//                                                " channelId " + channelId+
//                                                " askOrderNum" + askOrderNum+
//                                                " bidOrderNum " + bidOrderNum
//                                );
//                                refillNum++;
                                    continue;
                                }
                            }
                            Integer bsFlag = transactionDataInfoInstance.getBSFlag();
                            if (bsFlag==66){
                                try {
                                    transBuyQueue.put(transInfoArray);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }else if(bsFlag==83){
                                try {
                                    transSellQueue.put(transInfoArray);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        countNum++;
                    }
                }
            }

        }
    }
}
