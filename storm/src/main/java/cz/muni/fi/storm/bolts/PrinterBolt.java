package cz.muni.fi.storm.bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.FileOutput;
import cz.muni.fi.storm.SimpleFileOutput;
import java.util.Map;
import java.util.logging.Logger;

public class PrinterBolt extends BaseBasicBolt {

    private static final Logger log = Logger.getLogger(PrinterBolt.class.getName());
    private String fileName;
    private FileOutput fileOut = null;

    public void Printer(String fileName) {
        this.fileName = fileName;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        log.fine("Preparing output File - prepare method Printer");
        fileOut = new SimpleFileOutput(fileName);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        fileOut.append(tuple.toString());
    }
}
