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
import java.util.HashMap;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class AggregationBolt extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Long> counter;
    private JSONParser parser;
    private String aggregateBy;

    public AggregationBolt(String aggregateBy) {
        this.aggregateBy = aggregateBy;
    }
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counter = new HashMap<String, Long>();
        this.parser = new JSONParser();
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            JSONObject jsonObject = (JSONObject) parser.parse(tuple.getString(0));
            String aggregateValue = (String) jsonObject.get(aggregateBy);
            Long last = counter.get(aggregateValue);
            if (last == null) last = new Long(0);
            last++;
            counter.put(aggregateValue, last);
        }
        catch (ParseException pe){
            //nothig
        }
    }
    
    @Override
    public void cleanup() {
        PrintWriter writer;
        try {
            writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File("/root/stormisti/a"), true)));
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Target file for flows doesnt exist", e);
        }
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            writer.println(entry.getKey() + " " + entry.getValue());
        }
        writer.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
