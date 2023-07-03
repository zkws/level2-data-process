package com.alphainv.tdfapi;

public class OrderDataInfo {
    private String exchangeTime;
    private String stockCode;
    private Character stkFunctionCode;
    private Character stkOrderKind;
    private Integer orderVolume;
    private Integer orderCode;
    private Integer channelId;
    private Long orderPrice;
    private Long orderOriNo;

    OrderDataInfo(String exchangeTime, String stockCode, Character stkFunctionCode, Character stkOrderKind, Integer channelId, Integer orderVolume, Integer orderCode, Long orderPrice, Long orderOriNo){
        this.stkFunctionCode = stkFunctionCode;
        this.stkOrderKind = stkOrderKind;
        this.channelId = channelId;
        this.exchangeTime = exchangeTime;
        this.stockCode = stockCode;
        this.orderVolume = orderVolume;
        this.orderCode = orderCode;
        this.orderPrice = orderPrice;
        this.orderOriNo = orderOriNo;
    }

    public String getExchangeTime() {
        return exchangeTime;
    }

    public String getStockCode() {
        return stockCode;
    }

    public Character getStkFunctionCode() {
        return stkFunctionCode;
    }

    public Character getStkOrderKind() {
        return stkOrderKind;
    }

    public Integer getOrderVolume() {
        return orderVolume;
    }

    public Integer getOrderCode() {
        return orderCode;
    }

    public Integer getChannelId() {
        return channelId;
    }

    public Long getOrderPrice() {
        return orderPrice;
    }

    public Long getOrderOriNo() {
        return orderOriNo;
    }
}
