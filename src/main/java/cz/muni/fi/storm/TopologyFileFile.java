package cz.muni.fi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import cz.muni.fi.storm.bolts.FileWriterBolt;
import cz.muni.fi.storm.bolts.GlobalCountWindowBolt;
import cz.muni.fi.storm.spouts.FileReaderSpout;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.TopologyUtil;

public class TopologyFileFile {

    public static void main(String[] args) {        
        if (args.length < 1) {
            throw new IllegalArgumentException("Missing argument: number_of_computers");
        }

        int numberOfComputers = Integer.parseInt(args[0]);
        
        IRichBolt globalCountWindowBolt = new GlobalCountWindowBolt();
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("fileReaderSpout", new FileReaderSpout(), numberOfComputers);
        builder.setBolt("fileWriterBolt", new FileWriterBolt(), numberOfComputers)
                .localOrShuffleGrouping("fileReaderSpout");
        builder.setBolt("globalCountWindowBolt", globalCountWindowBolt)
                .globalGrouping("fileWriterBolt", ServiceCounter.getStreamIdForService());

        Config config = new Config();
        config.setNumWorkers(numberOfComputers);
        config.putAll(new TopologyUtil().loadProperties());

        try {
            StormSubmitter.submitTopology("TopologyFileFile", config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Couldn't initialize the topology", e);
        }
    }
}
