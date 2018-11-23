package com.zeek.mystorm.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.com.google.common.collect.ImmutableList;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName TridentTopologyApp
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 上午10:33
 * @Version v1.0
 **/
public class TridentTopologyApp {

    public static void main(String[] args) throws Exception {

        System.out.println("开始测试");

        Config config = new Config();
        config.setNumAckers(3);
        Map<String, String> map = new HashMap<String, String>();
        map.put("storm.zookeeper.servers", "192.168.56.100");//使用s100不行
        config.setEnvironment(map);

        // 创建top
        TridentTopology top = new TridentTopology();

        // 创建spout
        FeederBatchSpout testSpout = new FeederBatchSpout(ImmutableList.of("a", "b", "c", "d"));

        // 创建流
        Stream s = top.newStream("spout", testSpout);
        // shuffle分组
        s.shuffle().each(new Fields("a", "b"), new CheckEvenSumFilter()).parallelismHint(2)
        .shuffle().each(new Fields("a", "b"), new SumFunction(), new Fields("sum")).parallelismHint(2);

        //本地集群模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormApp", config, top.build());

        testSpout.feed(ImmutableList.of(new Values(1, 2, 3, 4)));
        testSpout.feed(ImmutableList.of(new Values(2, 2, 4, 5)));
        testSpout.feed(ImmutableList.of(new Values(4, 4, 5, 6)));
        testSpout.feed(ImmutableList.of(new Values(4, 5, 6, 7)));

        Thread.sleep(10000);
        cluster.shutdown();
    }


}
