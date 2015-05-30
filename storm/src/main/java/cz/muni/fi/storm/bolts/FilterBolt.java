package cz.muni.fi.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.math.BigInteger;
import java.util.Map;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JSONParser jsonParser;
    private BigInteger counter = new BigInteger("0");
    private String key;
    private String value;
    
    public FilterBolt(String key, String value) {
        this.key = key;
        this.value = value;
    }
    
    @Override
    public void cleanup() {}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        jsonParser = new JSONParser();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filtered-flow"));
    }

    @Override
    public void execute (Tuple tuple) {
        String flow = tuple.getString(0);
        
        try {
            JSONObject jFlow = (JSONObject) jsonParser.parse(flow);
            Object containValue = jFlow.get(this.key);
            if (containValue.toString().equals(this.value)) {
                counter = counter.add(BigInteger.ONE);
                this.collector.emit(new Values(flow));
            }
        } catch (ParseException e) {
            // nothing
        }

        //collector.ack(tuple);
    }
}
