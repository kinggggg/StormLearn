package com.zeek.mystorm.drpc.local;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.utils.DRPCClient;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName MyLocalDRPC
 * @Description
 * @Author liweibo
 * @Date 2018/12/3 下午11:33
 * @Version v1.0
 **/
public class MyRemoteDRPC {

    public static void main(String[] args) throws Exception {

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);

        Config config = new Config();
        config.setDebug(false);
        config.put("strom.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576);


        Map<String, String> map = new HashMap<String, String>();
        map.put("storm.zookeeper.servers", "192.168.56.100");//使用s100不行
        map.put("drpc.servers", "s100");
        config.setEnvironment(map);

        StormSubmitter.submitTopology("exclamation-drpc", config, builder.createRemoteTopology());

        DRPCClient client = new DRPCClient(config, "s100", 3772);
        System.out.println(client.execute("exclamation", "3,4"));



    }
}
