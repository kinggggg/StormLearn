package com.zeek.mystorm.trident.aggregate;

import clojure.lang.Numbers;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @ClassName SumCombinerAggregator
 * @Description
 * @Author liweibo
 * @Date 2018/11/28 上午12:08
 * @Version v1.0
 **/
public class SumCombinerAggregator implements CombinerAggregator<Number> {
    private static final long serialVersionUID = 1L;

    public Number init(TridentTuple tridentTuple) {
        System.out.println(this + " init(" + tridentTuple.getInteger(0) + ")");
        return (Number) tridentTuple.getValue(0);
    }

    public Number combine(Number number1, Number number2) {
        System.out.println(this + " combine(" + number1 + "," + number2 + ")");
        return Numbers.add(number1, number2);
    }

    public Number zero() {
        System.out.println(this + " zero()");
        return 0;
    }
}