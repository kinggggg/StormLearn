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
public class SumFunction extends BaseFunction {
    private static final long serialVersionUID = 5L;

    public void execute(TridentTuple tuple, TridentCollector
            collector) {
        int number1 = tuple.getInteger(0);
        int number2 = tuple.getInteger(1);
        int sum = number1 + number2;
// emit the sum of first two fields
        collector.emit(new Values(sum));
    }
}