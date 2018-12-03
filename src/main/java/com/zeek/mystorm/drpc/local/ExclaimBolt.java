package com.zeek.mystorm.drpc.local;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * @ClassName ExclaimBolt
 * @Description
 * @Author liweibo
 * @Date 2018/12/3 下午11:39
 * @Version v1.0
 **/
public class ExclaimBolt extends BaseBasicBolt {
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String input = tuple.getString(1);
        String[] split = input.split(",");
        int a = Integer.parseInt(split[0]);
        int b = Integer.parseInt(split[1]);
        collector.emit(new Values(tuple.getValue(0), a + b));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "result"));
    }
}