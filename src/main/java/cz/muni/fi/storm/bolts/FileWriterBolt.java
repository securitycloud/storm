package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

public class FileWriterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private File targetFile;
    private PrintWriter writer = null;

    public FileWriterBolt(String filePath) {
        this.targetFile = new File(filePath);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            this.writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(targetFile, true)));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Target file for flows doesnt exist", e);
        }
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String flow = tuple.getValue(0).toString();
        append(flow);
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        writer.close();
    }

    private void append(String string) {
        writer.println(string);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
