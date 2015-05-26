package cz.muni.fi.storm;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.PacketCounter;
import cz.muni.fi.storm.spouts.*;


public class TopologyMain {
    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        
       // String topicName = "securitycloud-testing-data";
       // String zK = "localhost:2181";
        

        /* Topology definition */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flows-reader", new FlowsReader(),1);
        
        //verification-topic len preto, ze u mna na locale som mal vytvoreny tento topic
       // builder.setSpout("flows-reader", new ConsumerGroupExample("verification-topic", "localhost:218","1"));
        builder.setBolt("packet-counter", new PacketCounter(),3)
                .fieldsGrouping("flows-reader", new Fields("flow"));

        
        /* Configuration */
        Config conf = new Config();
        conf.setNumWorkers(4);
        conf.put("flowsFile", "/root/megaOut");
        conf.put("outputFile", "/root/stormisti/workdir/result");
        conf.setDebug(false);
       
        conf.put("kafka.spout.topic", "securitycloud-testing-data");
        conf.put("kafka.zookeeper.connect","localhost:2181");
        
        //SpoutConfig spoutConfig = new SpoutConfig();
        
        
        /* Topology run */
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        //LocalCluster cluster = new LocalCluster();
        //cluster.submitTopology("Flows-Topology", conf, builder.createTopology());
        StormSubmitter.submitTopology("Flows-Topology", conf, builder.createTopology());
    }
}
