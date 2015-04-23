package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.FlowsNormalizer;
import cz.muni.fi.storm.spouts.FlowsReader;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {

        /* Topology definition */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flows-reader", new FlowsReader());
        builder.setBolt("flows-normalizer", new FlowsNormalizer())
                .fieldsGrouping("flows-reader", new Fields("line"));

        /* Configuration */
        Config conf = new Config();
        conf.put("flowsFile", args[0]);
        conf.setDebug(false);

        /* Topology run */
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Flows-Toplogie", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.shutdown();
    }
}
