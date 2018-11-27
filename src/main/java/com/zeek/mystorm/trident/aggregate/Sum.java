package com.zeek.mystorm.trident.aggregate;

import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * @ClassName Sum
 * @Description 自定义sum聚合函数
 * @Author liweibo
 * @Date 2018/11/27 下午10:50
 * @Version v1.0
 **/
public class Sum implements ReducerAggregator<Long> {
    private static final long serialVersionUID = 1L;

    //return the initial value zero
    public Long init() {
        return 0L;
    }

    //Iterates on the input tuples, calculate the sum and
    //produce the single tuple with single field as output
    public Long reduce(Long curr, TridentTuple tuple) {
        return curr + tuple.getInteger(0) + tuple.getInteger(1);
    }
}