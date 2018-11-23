package com.zeek.mystorm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @ClassName CheckEvenSumFilter
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 上午10:32
 * @Version v1.0
 **/
public class CheckEvenSumFilter extends BaseFilter {
    private static final long serialVersionUID = 7L;
    public boolean isKeep(TridentTuple tuple) {
        int a = tuple.getInteger(0);
        int b = tuple.getInteger(1);
//        int c = tuple.getInteger(2);
//        int d = tuple.getInteger(3);
        System.out.println("=============>");
        System.out.println("=============> a = " + a + ", b = " + b);
        System.out.println("=============>");
        int sum = a + b;
        if(sum % 2 == 0) {
            return true;
        }
        return false;
    }
}