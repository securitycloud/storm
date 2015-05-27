package cz.muni.fi.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.FileOutput;
import cz.muni.fi.storm.SimpleFileOutput;
import java.util.Map;

public class Printer extends BaseBasicBolt {

    private FileOutput fileOut = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        fileOut = new SimpleFileOutput((String)stormConf.get("outputFile"));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void execute (Tuple tuple, BasicOutputCollector collector) {
        fileOut.append(tuple.toString());
    }
}
