package com.zeek.mystorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @ClassName CallLogCreatorBolt
 * @Description
 * @Author liweibo
 * @Date 2018/11/22 下午5:19
 * @Version v1.0
 **/
public class CallLogCreatorBolt implements IRichBolt {
    //Create instance for OutputCollector which collects and emits tuples to produce output
    private OutputCollector collector;

    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        System.out.println("Bolt1.prepare()");
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        String from = tuple.getString(0);
        String to = tuple.getString(1);
        Integer duration = tuple.getInteger(2);

//        System.out.println("Bolt1.execute:" + from + ", " + to + ", " +  duration);
        collector.emit(new Values(from + " - " + to, duration));
    }

    public void cleanup() {
        System.out.println("Bolt1.cleanup()");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call", "duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}