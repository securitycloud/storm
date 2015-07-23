package cz.muni.fi.storm.spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cz.muni.fi.storm.tools.readers.FileReader;
import cz.muni.fi.storm.tools.readers.Reader;
import java.util.Map;

public class FileReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String filePath;
    private Reader fileReader;

    public FileReaderSpout(String filePath) {
        this.filePath = filePath;
    }
    
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.fileReader = new FileReader(filePath);
        this.collector = collector;
    }
    
    @Override
    public void nextTuple() {
        String flow = fileReader.next();
        if (flow != null) {
            this.collector.emit(new Values(flow));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
    
    @Override
    public void close() {
        fileReader.close();
    }
}
