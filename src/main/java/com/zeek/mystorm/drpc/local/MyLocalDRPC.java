package com.zeek.mystorm.drpc.local;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * @ClassName MyLocalDRPC
 * @Description
 * @Author liweibo
 * @Date 2018/12/3 下午11:33
 * @Version v1.0
 **/
public class MyLocalDRPC {

    public static void main(String[] args) {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt());

        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));

        System.out.println("Results for 'hello':" + drpc.execute("exclamation", "1,2"));

        cluster.shutdown();
        drpc.shutdown();

    }
}
