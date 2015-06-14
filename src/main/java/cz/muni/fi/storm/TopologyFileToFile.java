package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import cz.muni.fi.storm.bolts.FileWriterBolt;
import cz.muni.fi.storm.spouts.FileReaderSpout;
import java.util.logging.Logger;

public class TopologyFileToFile {

    private static final Logger log = Logger.getLogger(TopologyFileToFile.class.getName());

    public static void main(String[] args) {
        log.fine("Starting: Topology-file-to-file");
        
        if (args.length < 2) {
            throw new IllegalArgumentException("Missing argument: source_file target_file");
        }

        String sourceFilePath = args[0];
        String targetFilePath = args[1];
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("file-reader-spout", new FileReaderSpout(sourceFilePath));
        builder.setBolt("file-writer-bolt", new FileWriterBolt(targetFilePath))
                .fieldsGrouping("file-reader-spout", new Fields("flow"));

        Config config = new Config();
        config.setNumWorkers(2);

        try {
            StormSubmitter.submitTopology("Topology-file-to-file", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
