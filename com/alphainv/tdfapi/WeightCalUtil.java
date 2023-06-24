package com.alphainv.tdfapi;

public class WeightCalUtil {


    public static Double getOrderPriceWeight(Long currentPrice, Long orderPrice){
        Double priceWeight = 1D;
        if (!currentPrice.equals(0L)){
            double orderPriceBias = Math.abs(((double) (orderPrice-currentPrice))/currentPrice);
            priceWeight = 1/(100*orderPriceBias+1);
        }
        return priceWeight;
    }

    public static void main(String[] args) {
        System.out.println(getOrderPriceWeight(100L,99L));
    }
}
