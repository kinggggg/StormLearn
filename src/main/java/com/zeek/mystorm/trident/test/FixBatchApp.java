package com.zeek.mystorm.trident.test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

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
                .shuffle().each(new Fields("a", "b"), null).parallelismHint(2);

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormApp", config, top.build());
        Thread.sleep(10000);
        cluster.shutdown();

    }
}
