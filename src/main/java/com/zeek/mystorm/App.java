package com.zeek.mystorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName App
 * @Description
 * @Author liweibo
 * @Date 2018/11/22 下午5:10
 * @Version v1.0
 **/
public class App {

    public static void main(String[] args) throws Exception {
        //Create Config instance for cluster configuration
        Config config = new Config();
        Map<String, String> map = new HashMap<String, String>();
        map.put("storm.zookeeper.servers", "192.168.56.100");//使用s100不行
        config.setEnvironment(map);
        config.setDebug(true);
//
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
                .shuffleGrouping("call-log-reader-spout");

        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
                .fieldsGrouping("call-log-creator-bolt", new Fields("call"));

        //本地集群模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("StormApp", config, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();

//        StormSubmitter.submitTopology("StormApp", config, builder.createTopology());
    }
}
