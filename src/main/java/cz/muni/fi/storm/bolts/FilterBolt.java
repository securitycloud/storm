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

// imports for Jackson
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FilterBolt extends BaseRichBolt {

    private OutputCollector collector;
    private JsonParser jsonParser;
    private BigInteger counter = new BigInteger("0");
    private String key;
    private String value;
    private transient ObjectMapper objectMapper = new ObjectMapper();
    
    public FilterBolt(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        //jsonParser = new JsonParser();
    }

    @Override
    public void execute (Tuple tuple) {
        try {
            String flow = tuple.getString(0);
            
            /* try {
            JSONObject jFlow = (JSONObject) jsonParser.parse(flow);
            Object containValue = jFlow.get(this.key);
                    
            if (containValue.toString().equals(this.value)) {
            counter = counter.add(BigInteger.ONE);
            this.collector.emit(new Values(flow));
            }
            } catch () {
            // nothing
            }*/
            
            JSONObject jFlow= objectMapper.convertValue(flow, JSONObject.class);
            //JSONObject jFlow= objectMapper.readValue(flow, JSONObject.class);
            Object containValue=jFlow.get(this.key);
            
            if (containValue.toString().equals(this.value)) {
            counter = counter.add(BigInteger.ONE);
            this.collector.emit(new Values(flow));
            
            
        }
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(FilterBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
        collector.ack(tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("flow"));
    }
}
