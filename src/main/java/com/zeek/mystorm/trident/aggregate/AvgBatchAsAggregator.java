package com.zeek.mystorm.trident.aggregate;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * @ClassName AvgBatchAsAggregator
 * @Description
 * @Author liweibo
 * @Date 2018/11/27 下午11:13
 * @Version v1.0
 **/
public class AvgBatchAsAggregator extends
        BaseAggregator<AvgBatchAsAggregator.State> {
    private static final long serialVersionUID = 1L;

    // state class
    static class State {
        //元组的总和
        float sum = 0;
        //元组的个数
        int count = 0 ;
    }

    // Initialize the state
    public State init(Object batchId, TridentCollector collector) {
        System.out.println(this + " init()");
        return new State();
    }

    // 在stat变量中维护状态
    public void aggregate(State state, TridentTuple tridentTuple,
                          TridentCollector tridentCollector) {
        System.out.println(this + " aggregate(" + state.sum + ")");
        state.count = state.count + 1;
        state.sum = state.sum + tridentTuple.getInteger(0);

    }

    // 处理完成所有元组之后，返回具有一个单个值的tuple
    public void complete(State state, TridentCollector tridentCollector) {
        System.out.println(this + " complete(" + state.count + ")");
        tridentCollector.emit(new Values(state.sum / state.count));
    }
}