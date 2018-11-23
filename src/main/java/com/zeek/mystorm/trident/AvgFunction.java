package com.zeek.mystorm.trident;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * @ClassName SumFunction
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 上午10:32
 * @Version v1.0
 **/
public class AvgFunction extends BaseFunction {
    private static final long serialVersionUID = 5L;

    public void execute(TridentTuple tuple, TridentCollector
            collector) {
        int a = tuple.getInteger(0);
        int b = tuple.getInteger(1);
        int c = tuple.getInteger(2);
        int d = tuple.getInteger(3);
        int sum = tuple.getInteger(4);
        float avg = (float)( a + b + c + d + sum) / 5;
        System.out.println("avg =============> avg = " + avg);
        collector.emit(new Values(sum));
    }
}