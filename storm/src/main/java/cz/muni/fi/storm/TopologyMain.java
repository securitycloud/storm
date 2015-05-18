package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.PacketCounter;
import cz.muni.fi.storm.spouts.FlowsReader;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {

        /* Topology definition */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flows-reader", new FlowsReader());
        builder.setBolt("packet-counter", new PacketCounter())
                .fieldsGrouping("flows-reader", new Fields("flow"));

        /* Configuration */
        Config conf = new Config();
        conf.put("flowsFile", "/mnt/data/radozaj/Masarykova univerzita/Magisterske studium/diplomovka/out");
        conf.put("outputFile", "/home/radozaj/result");
        conf.setDebug(false);

        /* Topology run */
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("Flows-Toplogie", conf, builder.createTopology());
        //Thread.sleep(180000);
        //cluster.shutdown();
        
        System.setProperty("storm.jar","/home/radozaj/NetBeansProjects/storm/storm/target/storm-1.0-SNAPSHOT.jar" );
        StormSubmitter.submitTopology("Flows-Topology", conf, builder.createTopology());
    }
}
