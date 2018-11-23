package com.zeek.mystorm.trident.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName FixBatchApp
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 下午2:11
 * @Version v1.0
 **/
public class FixBatchApp {

    public static void main(String[] args) throws Exception {

        FixedBatchSpout spout = new FixedBatchSpout(new Fields("a", "b"), 3,
                new Values(1, 2),
                new Values(2, 2),
                new Values(3, 2),
                new Values(4, 2));
        spout.setCycle(true);

        TridentTopology top = new TridentTopology();
        top.newStream("tx-1", spout)
                //创建2个task，由于Filter1采取的是随机的分组策略，Filter1的两个实例均会收到数据
                .shuffle().each(new Fields("a", "b"), new Filter1()).parallelismHint(2)
                //创建3个task，由于Filter2采取的 global的分组策略，只有一个Filter2的实例会收到数据
                .global().each(new Fields("a", "b"), new Filter2()).parallelismHint(3);

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormApp", config, top.build());
        Thread.sleep(10000000);
        cluster.shutdown();

    }
}
