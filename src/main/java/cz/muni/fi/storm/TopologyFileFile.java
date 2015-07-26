package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.FileWriterBolt;
import cz.muni.fi.storm.spouts.FileReaderSpout;
import cz.muni.fi.storm.tools.TopologyUtil;

public class TopologyFileFile {

    public static void main(String[] args) {        
        if (args.length < 5) {
            throw new IllegalArgumentException("Missing argument: source_file target_file number_of_computers * kafka_consumer_ip");
        }

        String sourceFilePath = args[0];
        String targetFilePath = args[1];
        int numberOfComputers = Integer.parseInt(args[2]);
        // args[3] is ignored
        String kafkaConsumerIp = args[4];
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("file-reader-spout", new FileReaderSpout(sourceFilePath), numberOfComputers);
        builder.setBolt("file-writer-bolt", new FileWriterBolt(targetFilePath, true), numberOfComputers)
                .localOrShuffleGrouping("file-reader-spout");

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.put("serviceCounter.ip", kafkaConsumerIp);
        config.put("serviceCounter.port", "9092");
        config.putAll(new TopologyUtil().loadProperties());

        try {
            StormSubmitter.submitTopology("TopologyFileFile", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
