package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import cz.muni.fi.storm.tools.ServiceCounter;
import cz.muni.fi.storm.tools.writers.FileWriter;
import cz.muni.fi.storm.tools.writers.Writer;
import java.util.Map;

public class FileWriterBolt extends BaseRichBolt {

    private String filePath;
    private Writer fileWriter;
    private boolean isCountable;
    private ServiceCounter counter;

    public FileWriterBolt(String filePath, boolean isCountable) {
        this.filePath = filePath;
        this.isCountable = isCountable;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.fileWriter = new FileWriter(filePath);
        if (isCountable) {
            String broker = (String) stormConf.get("kafkaProducer.broker");
            int port = new Integer(stormConf.get("kafkaProducer.port").toString());
            this.counter = new ServiceCounter(broker, port);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String flow = tuple.getValue(0).toString();
        fileWriter.send(flow);
        if (isCountable) {
            counter.count();
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        fileWriter.close();
    }
}
