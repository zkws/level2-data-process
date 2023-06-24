package com.alphainv.tdfapi;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedOrder implements Delayed {
    private String stkCode;
    private Long orderTurnover;
    private long avaibleTime;

    public DelayedOrder(String stkCode, Long orderTurnover, long delayTime){
        this.stkCode=stkCode;
        //avaibleTime = 当前时间+ delayTime
        this.orderTurnover=orderTurnover;
        this.avaibleTime=delayTime + System.currentTimeMillis();
    }

    public long getAvaibleTime() {
        return avaibleTime;
    }

    public String getStkCode() {
        return stkCode;
    }

    public Long getOrderTurnover() {
        return orderTurnover;
    }

    @Override
    public long getDelay(TimeUnit timeUnit) {
        long diffTime= avaibleTime- System.currentTimeMillis();
        return timeUnit.convert(diffTime,TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed delayed) {
        return (int)(this.avaibleTime - ((DelayedOrder) delayed).getAvaibleTime());
    }
}
