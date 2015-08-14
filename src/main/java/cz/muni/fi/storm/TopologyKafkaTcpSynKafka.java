package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.bolts.TcpSynMoreThan10PackKafkaBolt;
import cz.muni.fi.storm.bolts.TcpSynPacketCounterBolt;
import cz.muni.fi.storm.spouts.KafkaSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;
import cz.muni.fi.storm.tools.TupleUtils;
import java.util.logging.Logger;

public class TopologyKafkaTcpSynKafka{

    private static final Logger log = Logger.getLogger(TopologyKafkaTopNKafka.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: TopologyKafkaTcpSyn");
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: number_of_computers from_beginning");
        }
        
        int numberOfComputers = Integer.parseInt(args[0]);        
        boolean fromBeginning = ("true".equals(args[1])) ? true : false;

        IRichSpout kafkaSpout = new KafkaSpout(fromBeginning, true);
        IRichBolt tcpSynPacketCounterBolt = new TcpSynPacketCounterBolt();
        IRichBolt tcpSynGlobalSortKafkaBolt = new TcpSynMoreThan10PackKafkaBolt(numberOfComputers);
        IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafkaSpout", kafkaSpout, numberOfComputers);
        builder.setBolt("tcpSynPacketCounterBolt", tcpSynPacketCounterBolt, numberOfComputers)
                .localOrShuffleGrouping("kafkaSpout")
                .localOrShuffleGrouping("kafkaSpout", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("globalSortKafkaBolt", tcpSynGlobalSortKafkaBolt)
                .globalGrouping("tcpSynPacketCounterBolt")
                .globalGrouping("tcpSynPacketCounterBolt", TupleUtils.getStreamIdForEndOfWindow());
        builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                .globalGrouping("tcpSynPacketCounterBolt", ServiceCounter.getStreamIdForService());

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());

        try {
            StormSubmitter.submitTopology("TopologyKafkaTcpSyn", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
