package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.ServiceCounter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;

public class FileWriterBolt extends BaseRichBolt {

    private File targetFile;
    private PrintWriter writer = null;
    private boolean isCountable;
    private ServiceCounter counter;

    public FileWriterBolt(String filePath, boolean isCountable) {
        this.targetFile = new File(filePath);
        this.isCountable = isCountable;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            this.writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(targetFile, true)));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Target file for flows doesnt exist", e);
        }
        if (isCountable) {
            this.counter = new ServiceCounter(stormConf.get("serviceCounter.ip").toString(),
                                              new Integer(stormConf.get("serviceCounter.port").toString()));
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String flow = tuple.getValue(0).toString();
        append(flow);
        if (isCountable) {
            counter.count();
        }
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
