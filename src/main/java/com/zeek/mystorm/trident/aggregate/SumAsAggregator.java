package com.zeek.mystorm.trident.aggregate;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * @ClassName SumAsAggregator
 * @Description
 * @Author liweibo
 * @Date 2018/11/27 下午11:13
 * @Version v1.0
 **/
public class SumAsAggregator extends
        BaseAggregator<SumAsAggregator.State> {
    private static final long serialVersionUID = 1L;

    // state class
    static class State {
        long count = 0;
    }

    // Initialize the state
    public State init(Object batchId, TridentCollector collector) {
        System.out.println(this + " init()");
        return new State();
    }

    // Maintain the state of sum into count variable.
    public void aggregate(State state, TridentTuple tridentTuple,
                          TridentCollector tridentCollector) {
        System.out.println(this + " aggregate(" + state.count + ")");
        state.count = tridentTuple.getInteger(0) + state.count;
    }

    // return a tuple with single value as output
    // after processing all the tuples of given batch.
    public void complete(State state, TridentCollector tridentCollector) {
        System.out.println(this + " complete(" + state.count + ")");
        tridentCollector.emit(new Values(state.count));
    }
}